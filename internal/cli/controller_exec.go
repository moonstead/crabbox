package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/openclaw/crabbox/internal/prefixbuffer"
)

// Workspace exec is an opt-in adapter capability. The operator authorises
// argv prefixes at startup; a caller that owns a ready workspace may run an
// argv starting with one of them, with private stdin, on that workspace only.
const (
	controllerExecMaxBodyBytes      = 256 << 10
	controllerExecMaxStdinBytes     = 128 << 10
	controllerExecMaxOutputBytes    = 16 << 10
	controllerExecMaxArgs           = 256
	controllerExecMaxArgvBytes      = 32 << 10
	controllerExecMaxAllowPrefixes  = 16
	controllerExecMinTimeout        = time.Second
	controllerExecDefaultMaxTimeout = 15 * time.Minute
	controllerExecMaximumTimeout    = time.Hour
	controllerExecStatusLimitBytes  = 4 << 10
	controllerExecStatusGrace       = time.Second
)

var (
	errControllerExecSetup        = errors.New("workspace command could not be started")
	errControllerExecRegistration = errors.New("workspace registration generation changed")
)

type controllerExecRequest struct {
	Argv           []string `json:"argv"`
	StdinBase64    string   `json:"stdinBase64,omitempty"`
	TimeoutMs      int64    `json:"timeoutMs"`
	LeaseID        string   `json:"leaseId"`
	RegistrationID string   `json:"registrationId,omitempty"`
}

type controllerExecResponse struct {
	ExitCode        int    `json:"exitCode"`
	StdoutBase64    string `json:"stdoutBase64"`
	StderrBase64    string `json:"stderrBase64"`
	StdoutBytes     int64  `json:"stdoutBytes"`
	StderrBytes     int64  `json:"stderrBytes"`
	StdoutTruncated bool   `json:"stdoutTruncated"`
	StderrTruncated bool   `json:"stderrTruncated"`
	DurationMs      int64  `json:"durationMs"`
}

// controllerExecCommand is one admitted command. Stdin is private: it must
// never be logged, persisted, or placed in argv or the environment.
type controllerExecCommand struct {
	Argv           []string
	Stdin          []byte
	RegistrationID string
}

// controllerWorkspaceExecRunner runs an admitted argv on the exact lease the
// adapter recorded, keeping provider and SSH credentials on the adapter host.
// It returns the command's exit status only when the command itself ran.
type controllerWorkspaceExecRunner interface {
	ExecWorkspaceCommand(ctx context.Context, request controllerWorkspaceRequest, command controllerExecCommand, stdout, stderr io.Writer) (int, error)
}

// controllerExecSupportChecker reports, without contacting the provider,
// whether claim-fenced execution is available for the configured route.
type controllerExecSupportChecker interface {
	ExecSupported(context.Context) (bool, error)
}

// verifyControllerExecSupport keeps an adapter with exec enabled from starting
// on a route where every command would fail.
func verifyControllerExecSupport(ctx context.Context, runner controllerWorkspaceRunner, provider string) error {
	_, runs := runner.(controllerWorkspaceExecRunner)
	checker, checks := runner.(controllerExecSupportChecker)
	if !runs || !checks {
		return fmt.Errorf("this adapter runner cannot execute workspace commands")
	}
	supported, err := checker.ExecSupported(ctx)
	if err != nil {
		return fmt.Errorf("check workspace exec support: %w", err)
	}
	if !supported {
		return fmt.Errorf("provider=%s does not support claim-fenced execution; remove --exec-allow", provider)
	}
	return nil
}

// parseControllerExecAllow parses one operator-authorised argv prefix.
func parseControllerExecAllow(value string, prefixes [][]string) ([][]string, error) {
	if len(prefixes) >= controllerExecMaxAllowPrefixes {
		return nil, fmt.Errorf("at most %d --exec-allow prefixes are allowed", controllerExecMaxAllowPrefixes)
	}
	var prefix []string
	decoder := json.NewDecoder(strings.NewReader(value))
	if err := decoder.Decode(&prefix); err != nil || decoder.More() {
		return nil, fmt.Errorf("--exec-allow must be one JSON array of strings")
	}
	if len(prefix) == 0 || len(prefix) > controllerExecMaxArgs {
		return nil, fmt.Errorf("--exec-allow must contain 1 to %d arguments", controllerExecMaxArgs)
	}
	for _, arg := range prefix {
		if arg == "" || strings.ContainsRune(arg, '\x00') || !utf8.ValidString(arg) {
			return nil, fmt.Errorf("--exec-allow arguments must be nonempty, NUL-free UTF-8")
		}
	}
	for _, existing := range prefixes {
		if slicesEqual(existing, prefix) {
			return nil, fmt.Errorf("--exec-allow prefix is defined more than once")
		}
	}
	return append(prefixes, prefix), nil
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// controllerExecAllowedPrefix returns the index of the first authorised
// prefix of argv, or -1.
func controllerExecAllowedPrefix(prefixes [][]string, argv []string) int {
	for i, prefix := range prefixes {
		if len(argv) >= len(prefix) && slicesEqual(argv[:len(prefix)], prefix) {
			return i
		}
	}
	return -1
}

func validateControllerExecArgv(argv []string) error {
	if len(argv) == 0 || len(argv) > controllerExecMaxArgs {
		return fmt.Errorf("argv must contain 1 to %d arguments", controllerExecMaxArgs)
	}
	if argv[0] == "" {
		return fmt.Errorf("argv[0] must be nonempty")
	}
	total := 0
	for _, arg := range argv {
		if strings.ContainsRune(arg, '\x00') || !utf8.ValidString(arg) {
			return fmt.Errorf("arguments must be NUL-free UTF-8")
		}
		total += len(arg)
	}
	if total > controllerExecMaxArgvBytes {
		return fmt.Errorf("argv exceeds %d bytes", controllerExecMaxArgvBytes)
	}
	return nil
}

func controllerExecArgvDigest(argv []string) string {
	sum := sha256.New()
	for _, arg := range argv {
		_, _ = io.WriteString(sum, arg)
		_, _ = sum.Write([]byte{0})
	}
	return hex.EncodeToString(sum.Sum(nil))
}

func (s *controllerService) execEnabled() bool {
	return len(s.opts.ExecAllow) > 0
}

func (s *controllerService) workspaceResponse(record controllerWorkspaceRecord) controllerWorkspaceResponse {
	response := controllerResponse(record)
	response.Capabilities.Exec = record.Request.Capabilities.Exec && s.execEnabled()
	return response
}

func (s *controllerService) workspaceExecSlot(id string) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	slot, ok := s.execSlots[id]
	if !ok {
		slot = make(chan struct{}, 1)
		s.execSlots[id] = slot
	}
	return slot
}

// execWorkspaceReadyLocked is checked at admission, when the side effect
// begins and before a result is released; callers hold s.mu. A lifecycle
// transition cancels a running command instead of waiting for this check.
func (s *controllerService) execWorkspaceReadyLocked(record controllerWorkspaceRecord, leaseID string) bool {
	_, retryPending := s.localCleanupRetry[record.Request.ID]
	_, revocationPending := s.terminalRevocation[record.Request.ID]
	return record.Status == "ready" && record.LeaseID == leaseID && IsCanonicalLeaseID(leaseID) &&
		record.Request.Capabilities.Exec && !record.LocalCleanupPending && !retryPending && !revocationPending &&
		!controllerWorkspaceExpired(controllerEffectiveExpiry(record, ""), s.now())
}

// cancelExecSideEffectsUnderGate stops commands whose workspace can no longer
// be used, including when its stopping transition could not be persisted.
func (s *controllerService) cancelExecSideEffectsUnderGate(id string, cause error) {
	for _, effect := range s.sideEffects {
		if effect.workspaceID == id && effect.exec {
			effect.cancel(cause)
		}
	}
}

func (s *controllerService) workspaceExec(w http.ResponseWriter, r *http.Request, id string) {
	runner, ok := s.runner.(controllerWorkspaceExecRunner)
	if !ok {
		writeControllerError(w, http.StatusNotImplemented, "exec_unsupported", "this adapter runner cannot execute workspace commands")
		return
	}
	if mediaType := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0])); mediaType != "application/json" {
		writeControllerError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}
	var request controllerExecRequest
	if err := decodeControllerExecJSON(w, r, &request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeControllerError(w, http.StatusRequestEntityTooLarge, "request_too_large", fmt.Sprintf("request body exceeds %d bytes", controllerExecMaxBodyBytes))
			return
		}
		// Decoder errors can quote input; never echo a body that may carry stdin.
		writeControllerError(w, http.StatusBadRequest, "invalid_request", "request must be one JSON object with argv, timeoutMs, leaseId and optional stdinBase64 and registrationId")
		return
	}
	stdinBase64 := request.StdinBase64
	request.StdinBase64 = ""
	if err := validateControllerExecArgv(request.Argv); err != nil {
		writeControllerError(w, http.StatusBadRequest, "invalid_argv", err.Error())
		return
	}
	policy := controllerExecAllowedPrefix(s.opts.ExecAllow, request.Argv)
	if policy < 0 {
		writeControllerError(w, http.StatusForbidden, "exec_argv_not_allowed", "argv does not start with a prefix this adapter authorises")
		return
	}
	if !IsCanonicalLeaseID(request.LeaseID) {
		writeControllerError(w, http.StatusBadRequest, "invalid_lease_id", "leaseId must be the workspace's canonical lease ID")
		return
	}
	if request.RegistrationID != "" && !validControllerWorkspaceID(request.RegistrationID) {
		writeControllerError(w, http.StatusBadRequest, "invalid_registration_id", "registrationId must be a lowercase DNS-style registration ID")
		return
	}
	if request.TimeoutMs < controllerExecMinTimeout.Milliseconds() || request.TimeoutMs > s.opts.ExecMaxTimeout.Milliseconds() {
		writeControllerError(w, http.StatusBadRequest, "invalid_timeout", fmt.Sprintf("timeoutMs must be between %d and %d", controllerExecMinTimeout.Milliseconds(), s.opts.ExecMaxTimeout.Milliseconds()))
		return
	}
	if base64.StdEncoding.DecodedLen(len(stdinBase64)) > controllerExecMaxStdinBytes+2 {
		writeControllerError(w, http.StatusRequestEntityTooLarge, "stdin_too_large", fmt.Sprintf("stdin exceeds %d bytes", controllerExecMaxStdinBytes))
		return
	}
	stdin, err := base64.StdEncoding.Strict().DecodeString(stdinBase64)
	stdinBase64 = ""
	if err != nil {
		clear(stdin)
		writeControllerError(w, http.StatusBadRequest, "invalid_stdin", "stdinBase64 must be standard base64")
		return
	}
	if len(stdin) > controllerExecMaxStdinBytes {
		clear(stdin)
		writeControllerError(w, http.StatusRequestEntityTooLarge, "stdin_too_large", fmt.Sprintf("stdin exceeds %d bytes", controllerExecMaxStdinBytes))
		return
	}
	defer clear(stdin)

	audit := controllerExecAudit{
		service: s, workspaceID: id, leaseID: request.LeaseID, registrationID: request.RegistrationID,
		policy: policy, argvDigest: controllerExecArgvDigest(request.Argv), argc: len(request.Argv),
		stdinBytes: len(stdin), started: s.now(),
	}
	record, ok := s.workspace(id)
	if !ok {
		writeControllerError(w, http.StatusNotFound, "workspace_not_found", "workspace not found")
		return
	}
	if !record.Request.Capabilities.Exec {
		audit.write("not_enabled", nil, nil, nil)
		writeControllerError(w, http.StatusForbidden, "exec_not_enabled", "workspace was not created with the exec capability")
		return
	}
	if record.LeaseID != request.LeaseID {
		audit.write("generation_mismatch", nil, nil, nil)
		writeControllerError(w, http.StatusConflict, "workspace_generation_mismatch", "leaseId does not match the workspace's current lease")
		return
	}
	s.mu.Lock()
	ready := s.execWorkspaceReadyLocked(record, request.LeaseID)
	s.mu.Unlock()
	if !ready {
		audit.write("not_ready", nil, nil, nil)
		writeControllerError(w, http.StatusConflict, "workspace_not_ready", "workspace must be ready, unexpired and not cleaning up")
		return
	}
	if !s.ensureDurableState(id) {
		s.scheduleReconcile(id)
		audit.write("durability_pending", nil, nil, nil)
		writeControllerError(w, http.StatusServiceUnavailable, "state_durability_pending", "controller state durability is pending")
		return
	}
	// A command never outlives its workspace.
	timeout := time.Duration(request.TimeoutMs) * time.Millisecond
	cappedByExpiry := false
	if expiresAt, ok := parseLeaseLabelTime(controllerEffectiveExpiry(record, "")); ok {
		if remaining := expiresAt.Sub(s.now()); remaining < timeout {
			timeout, cappedByExpiry = max(remaining, time.Nanosecond), true
		}
	}
	execCtx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	slot := s.workspaceExecSlot(id)
	select {
	case slot <- struct{}{}:
		defer func() { <-slot }()
	default:
		audit.write("busy", nil, nil, nil)
		writeControllerError(w, http.StatusConflict, "exec_busy", "another command is running in this workspace")
		return
	}
	// Commands have their own capacity so a long command cannot starve the
	// lifecycle reconciliation that would cancel it.
	select {
	case s.execSem <- struct{}{}:
		defer func() { <-s.execSem }()
	default:
		audit.write("capacity", nil, nil, nil)
		w.Header().Set("Retry-After", "1")
		writeControllerError(w, http.StatusTooManyRequests, "exec_capacity", "adapter command capacity is busy")
		return
	}
	leaseID := request.LeaseID
	effectCtx, effect, beginErr := s.beginExecSideEffect(execCtx, id, func(candidate controllerWorkspaceRecord) bool {
		return s.execWorkspaceReadyLocked(candidate, leaseID)
	})
	if beginErr != nil {
		if errors.Is(beginErr, errControllerStateDurabilityPending) {
			s.scheduleReconcile(id)
			audit.write("durability_pending", nil, nil, nil)
			writeControllerError(w, http.StatusServiceUnavailable, "state_durability_pending", "controller state durability is pending")
			return
		}
		audit.write("lifecycle_changed", nil, nil, nil)
		writeControllerError(w, http.StatusConflict, "workspace_not_ready", "workspace lifecycle changed before the command started")
		return
	}
	stdout := newControllerTailBuffer(controllerExecMaxOutputBytes)
	stderr := newControllerTailBuffer(controllerExecMaxOutputBytes)
	command := controllerExecCommand{Argv: request.Argv, Stdin: stdin, RegistrationID: request.RegistrationID}
	exitCode, runErr := runner.ExecWorkspaceCommand(effectCtx, controllerRequestForRecord(record), command, stdout, stderr)
	clear(stdin)
	effectCause := context.Cause(effectCtx)
	s.finishSideEffect(effect)
	switch {
	case errors.Is(effectCause, errControllerWorkspaceStopping):
		audit.write("lifecycle_changed", nil, stdout, stderr)
		writeControllerError(w, http.StatusConflict, "workspace_lifecycle_changed", "workspace lifecycle changed during the command; it was stopped and its output withheld")
		return
	case errors.Is(effectCause, errControllerStateDurabilityPending):
		s.scheduleReconcile(id)
		audit.write("durability_pending", nil, stdout, stderr)
		writeControllerError(w, http.StatusServiceUnavailable, "state_durability_pending", "controller state durability became pending during the command; it was stopped and its output withheld")
		return
	case r.Context().Err() != nil:
		audit.write("canceled", nil, stdout, stderr)
		return
	case errors.Is(context.Cause(execCtx), context.DeadlineExceeded):
		if cappedByExpiry {
			audit.write("expired", nil, stdout, stderr)
			writeControllerError(w, http.StatusConflict, "workspace_expired", "workspace expired during the command; it was stopped and its output withheld")
			return
		}
		audit.write("timeout", nil, stdout, stderr)
		writeControllerError(w, http.StatusGatewayTimeout, "exec_timeout", "command exceeded timeoutMs; it was stopped and its output withheld")
		return
	case errors.Is(runErr, errControllerExecRegistration):
		audit.write("generation_mismatch", nil, stdout, stderr)
		writeControllerError(w, http.StatusConflict, "workspace_generation_mismatch", "registrationId does not match the workspace's current registration")
		return
	case runErr != nil:
		if s.log != nil && !errors.Is(runErr, errControllerExecSetup) {
			fmt.Fprintf(s.log, "controller exec failed workspace=%s: %v\n", id, runErr)
		}
		audit.write("unavailable", nil, stdout, stderr)
		writeControllerError(w, http.StatusBadGateway, "exec_unavailable", "could not run the command on the workspace")
		return
	}
	// Serialize the final check with DELETE so a result is never released after
	// a concurrent stop transition.
	s.sideEffectGate.Lock()
	s.mu.Lock()
	final, ok := s.state.Workspaces[id]
	finalReady := ok && s.execWorkspaceReadyLocked(final, leaseID) && !s.durabilityPending && r.Context().Err() == nil
	if finalReady {
		audit.write("completed", &exitCode, stdout, stderr)
		writeControllerJSON(w, http.StatusOK, controllerExecResponse{
			ExitCode:        exitCode,
			StdoutBase64:    base64.StdEncoding.EncodeToString(stdout.Bytes()),
			StderrBase64:    base64.StdEncoding.EncodeToString(stderr.Bytes()),
			StdoutBytes:     stdout.Total(),
			StderrBytes:     stderr.Total(),
			StdoutTruncated: stdout.Truncated(),
			StderrTruncated: stderr.Truncated(),
			DurationMs:      s.now().Sub(audit.started).Milliseconds(),
		})
	}
	s.mu.Unlock()
	s.sideEffectGate.Unlock()
	if !finalReady {
		audit.write("lifecycle_changed", nil, stdout, stderr)
		writeControllerError(w, http.StatusConflict, "workspace_lifecycle_changed", "workspace lifecycle changed before the command result was released; its output was withheld")
	}
}

func decodeControllerExecJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, controllerExecMaxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request must contain one JSON object")
		}
		return err
	}
	return nil
}

func (s *controllerService) beginExecSideEffect(parent context.Context, id string, valid func(controllerWorkspaceRecord) bool) (context.Context, *controllerSideEffect, error) {
	ctx, effect, err := s.beginSideEffect(parent, id, false, true, true, valid)
	if err != nil {
		return nil, nil, err
	}
	s.sideEffectGate.Lock()
	effect.exec = true
	// A terminal revocation recorded between admission and registration must
	// still reach this command.
	s.mu.Lock()
	_, revocationPending := s.terminalRevocation[id]
	s.mu.Unlock()
	if revocationPending {
		effect.cancel(errControllerWorkspaceStopping)
	}
	s.sideEffectGate.Unlock()
	return ctx, effect, nil
}

// controllerExecAudit writes one metadata-only line per command attempt. It
// never records argv, stdin or output, only their sizes and an argv digest.
type controllerExecAudit struct {
	service        *controllerService
	workspaceID    string
	leaseID        string
	registrationID string
	policy         int
	argvDigest     string
	argc           int
	stdinBytes     int
	started        time.Time
}

func (a controllerExecAudit) write(outcome string, exitCode *int, stdout, stderr *controllerTailBuffer) {
	if a.service.log == nil {
		return
	}
	exit := "-"
	if exitCode != nil {
		exit = fmt.Sprint(*exitCode)
	}
	var stdoutBytes, stderrBytes int64
	if stdout != nil {
		stdoutBytes = stdout.Total()
	}
	if stderr != nil {
		stderrBytes = stderr.Total()
	}
	fmt.Fprintf(a.service.log, "controller exec workspace=%s lease=%s registration=%s policy=%d argc=%d argv_sha256=%s stdin_bytes=%d outcome=%s exit=%s stdout_bytes=%d stderr_bytes=%d duration=%s\n",
		a.workspaceID, a.leaseID, blank(a.registrationID, "-"), a.policy, a.argc, a.argvDigest, a.stdinBytes,
		outcome, exit, stdoutBytes, stderrBytes, a.service.now().Sub(a.started).Round(time.Millisecond))
}

// controllerTailBuffer keeps the last limit bytes written and counts the rest.
// A failing command's diagnosis is usually at the end of its output.
type controllerTailBuffer struct {
	limit int
	data  []byte
	total int64
}

func newControllerTailBuffer(limit int) *controllerTailBuffer {
	return &controllerTailBuffer{limit: limit}
}

func (b *controllerTailBuffer) Write(p []byte) (int, error) {
	b.total += int64(len(p))
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
		return len(p), nil
	}
	if overflow := len(b.data) + len(p) - b.limit; overflow > 0 {
		b.data = append(b.data[:0], b.data[overflow:]...)
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *controllerTailBuffer) Bytes() []byte   { return b.data }
func (b *controllerTailBuffer) Total() int64    { return b.total }
func (b *controllerTailBuffer) Truncated() bool { return b.total > int64(len(b.data)) }

// ExecSupported runs the offline `crabbox exec --check` for the configured
// provider route.
func (r *execControllerWorkspaceRunner) ExecSupported(ctx context.Context) (bool, error) {
	args := r.appendProviderArg([]string{"exec", "--check"}, controllerWorkspaceRequest{})
	output := prefixbuffer.NewLimited(controllerOutputLimitBytes)
	if err := r.runWithStarted(ctx, controllerWorkspaceRequest{}, args, &output, nil); err != nil {
		return false, err
	}
	if err := controllerOutputOverflowError(output.Exceeded(), "crabbox exec capability check", controllerOutputLimitBytes); err != nil {
		return false, err
	}
	var view struct {
		Execution bool `json:"execution"`
	}
	if err := json.Unmarshal(output.Bytes(), &view); err != nil {
		return false, fmt.Errorf("decode crabbox exec capability check: %w", err)
	}
	return view.Execution, nil
}

// ExecWorkspaceCommand runs `crabbox exec` for the workspace's recorded lease
// as a tracked child. Stdin reaches it only through a private pipe; a status
// pipe distinguishes the command's exit status from a setup failure.
func (r *execControllerWorkspaceRunner) ExecWorkspaceCommand(ctx context.Context, request controllerWorkspaceRequest, command controllerExecCommand, stdout, stderr io.Writer) (int, error) {
	leaseID := request.ProviderLeaseID
	if !IsCanonicalLeaseID(leaseID) || len(command.Argv) == 0 {
		return 0, fmt.Errorf("workspace command requires a canonical lease ID and argv")
	}
	args := []string{"exec", "--id", leaseID, "--status-fd", "5", "--terminate-remote-on-disconnect"}
	if command.RegistrationID != "" {
		args = append(args, "--expect-runtime-registration", command.RegistrationID)
	}
	args, err := r.appendPersistedProviderRoutingArgs(args, request)
	if err != nil {
		return 0, err
	}
	args = append(append(args, "--"), command.Argv...)
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		return 0, fmt.Errorf("create crabbox exec status pipe: %w", err)
	}
	defer statusReader.Close()
	statusDone := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(io.LimitReader(statusReader, controllerExecStatusLimitBytes))
		statusDone <- data
	}()
	input := command.Stdin
	if input == nil {
		input = []byte{}
	}
	runErr := r.runTrackedChild(ctx, request, args, controllerChildStreams{Stdout: stdout, Stderr: stderr, Input: input, Status: statusWriter}, nil, nil)
	var status []byte
	select {
	case status = <-statusDone:
	case <-time.After(controllerExecStatusGrace):
		// The process group is gone; a descriptor still open elsewhere must not
		// hold the request.
		_ = statusReader.Close()
		status = <-statusDone
	}
	started, rejected := controllerExecStatusEvents(status)
	if ctx.Err() != nil {
		return 0, context.Cause(ctx)
	}
	if rejected == "registration" {
		return 0, errControllerExecRegistration
	}
	if !started {
		// Crabbox diagnostics from a failed setup are not command output.
		return 0, errors.Join(errControllerExecSetup, runErr)
	}
	if runErr == nil {
		return 0, nil
	}
	var exitErr *controllerCommandError
	if errors.As(runErr, &exitErr) {
		return exitErr.ExitCode, nil
	}
	return 0, runErr
}

func controllerExecStatusEvents(data []byte) (started bool, rejected string) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var event execStatusEvent
		if json.Unmarshal(scanner.Bytes(), &event) != nil {
			continue
		}
		switch event.Event {
		case "started":
			started = true
		case "rejected":
			rejected = event.Reason
		}
	}
	return started, rejected
}
