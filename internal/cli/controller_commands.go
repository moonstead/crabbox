package cli

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/openclaw/crabbox/internal/prefixbuffer"
)

// Workspace commands are an opt-in adapter capability. The operator names each
// allowed argv at startup; callers choose a name and may send private stdin.
const (
	controllerCommandsVersion        = 1
	controllerCommandMaxStdinBytes   = 32 << 10
	controllerCommandMaxOutputBytes  = 16 << 10
	controllerCommandMaxArgs         = 32
	controllerCommandMaxArgBytes     = 4 << 10
	controllerCommandMaxArgvBytes    = 16 << 10
	controllerCommandMaxCount        = 16
	controllerCommandDefaultTimeout  = 2 * time.Minute
	controllerCommandMaximumDuration = time.Hour
)

// controllerWorkspaceCommandRunner is implemented by runners that can execute
// an operator-defined argv on a ready workspace without exposing credentials.
type controllerWorkspaceCommandRunner interface {
	RunWorkspaceCommand(ctx context.Context, leaseID string, request controllerWorkspaceRequest, argv []string, stdin []byte, stdout, stderr io.Writer) (int, error)
}

// controllerCommandSupportChecker reports, without contacting the provider,
// whether crabbox exec can run on the configured provider's leases.
type controllerCommandSupportChecker interface {
	CommandExecutionSupported(context.Context) (bool, error)
}

// verifyControllerCommandSupport keeps an adapter with commands from starting
// on a route where every command would fail with an ambiguous exit status.
func verifyControllerCommandSupport(ctx context.Context, runner controllerWorkspaceRunner, provider string) error {
	_, runs := runner.(controllerWorkspaceCommandRunner)
	checker, checks := runner.(controllerCommandSupportChecker)
	if !runs || !checks {
		return fmt.Errorf("this adapter runner cannot execute workspace commands")
	}
	supported, err := checker.CommandExecutionSupported(ctx)
	if err != nil {
		return fmt.Errorf("check workspace command support: %w", err)
	}
	if !supported {
		return fmt.Errorf("provider=%s does not support crabbox exec; remove --command", provider)
	}
	return nil
}

type controllerCommandRequest struct {
	StdinBase64    string `json:"stdinBase64,omitempty"`
	TimeoutSeconds int    `json:"timeoutSeconds,omitempty"`
}

type controllerCommandResponse struct {
	ExitCode        int    `json:"exitCode"`
	StdoutBase64    string `json:"stdoutBase64"`
	StderrBase64    string `json:"stderrBase64"`
	StdoutTruncated bool   `json:"stdoutTruncated"`
	StderrTruncated bool   `json:"stderrTruncated"`
}

// parseControllerCommandDefinition parses NAME=["/absolute/program","arg",...].
func parseControllerCommandDefinition(value string, commands map[string][]string) error {
	name, raw, ok := strings.Cut(value, "=")
	name = strings.TrimSpace(name)
	if !ok || !validControllerWorkspaceID(name) {
		return fmt.Errorf("--command must be NAME=JSON_ARGV with a lowercase DNS-style name")
	}
	if _, exists := commands[name]; exists {
		return fmt.Errorf("--command %s is defined more than once", name)
	}
	if len(commands) >= controllerCommandMaxCount {
		return fmt.Errorf("at most %d --command definitions are allowed", controllerCommandMaxCount)
	}
	var argv []string
	decoder := json.NewDecoder(strings.NewReader(raw))
	if err := decoder.Decode(&argv); err != nil || decoder.More() {
		return fmt.Errorf("--command %s argv must be one JSON array of strings", name)
	}
	if err := validateControllerCommandArgv(argv); err != nil {
		return fmt.Errorf("--command %s: %w", name, err)
	}
	commands[name] = argv
	return nil
}

func validateControllerCommandArgv(argv []string) error {
	if len(argv) == 0 || len(argv) > controllerCommandMaxArgs {
		return fmt.Errorf("argv must contain 1 to %d arguments", controllerCommandMaxArgs)
	}
	if !strings.HasPrefix(argv[0], "/") {
		return fmt.Errorf("argv[0] must be an absolute program path")
	}
	total := 0
	for _, arg := range argv {
		if arg == "" || len(arg) > controllerCommandMaxArgBytes || strings.ContainsRune(arg, '\x00') {
			return fmt.Errorf("arguments must be nonempty, NUL-free and at most %d bytes", controllerCommandMaxArgBytes)
		}
		total += len(arg)
	}
	if total > controllerCommandMaxArgvBytes {
		return fmt.Errorf("argv exceeds %d bytes", controllerCommandMaxArgvBytes)
	}
	return nil
}

func (s *controllerService) commandNames() []string {
	names := make([]string, 0, len(s.opts.Commands))
	for name := range s.opts.Commands {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// listCommands advertises the enabled contract. It never reveals argv.
func (s *controllerService) listCommands(w http.ResponseWriter) {
	writeControllerJSON(w, http.StatusOK, map[string]any{
		"version":           controllerCommandsVersion,
		"commands":          s.commandNames(),
		"maxStdinBytes":     controllerCommandMaxStdinBytes,
		"maxOutputBytes":    controllerCommandMaxOutputBytes,
		"maxTimeoutSeconds": int(s.opts.CommandTimeout / time.Second),
	})
}

func (s *controllerService) workspaceCommandSlot(id string) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	slot, ok := s.commandSlots[id]
	if !ok {
		slot = make(chan struct{}, 1)
		s.commandSlots[id] = slot
	}
	return slot
}

// commandWorkspaceReadyLocked is checked when the command is admitted and
// before its result is released; callers hold s.mu. A lifecycle transition
// cancels a running command instead.
func (s *controllerService) commandWorkspaceReadyLocked(record controllerWorkspaceRecord, leaseID string) bool {
	_, retryPending := s.localCleanupRetry[record.Request.ID]
	_, revocationPending := s.terminalRevocation[record.Request.ID]
	return record.Status == "ready" && record.LeaseID == leaseID && record.Request.Capabilities.Commands &&
		!record.LocalCleanupPending && !retryPending && !revocationPending &&
		!controllerWorkspaceExpired(controllerEffectiveExpiry(record, ""), s.now())
}

func (s *controllerService) runWorkspaceCommand(w http.ResponseWriter, r *http.Request, id, name string) {
	argv, ok := s.opts.Commands[name]
	if !ok {
		writeControllerError(w, http.StatusNotFound, "command_not_found", "command is not defined by this adapter")
		return
	}
	runner, ok := s.runner.(controllerWorkspaceCommandRunner)
	if !ok {
		writeControllerError(w, http.StatusNotImplemented, "commands_unsupported", "this adapter runner cannot execute workspace commands")
		return
	}
	if mediaType := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0])); mediaType != "application/json" {
		writeControllerError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}
	var request controllerCommandRequest
	if err := decodeControllerJSON(w, r, &request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeControllerError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds 64 KiB")
			return
		}
		// Decoder errors can quote input; never echo a request that may carry stdin.
		writeControllerError(w, http.StatusBadRequest, "invalid_request", "request must be one JSON object with optional stdinBase64 and timeoutSeconds")
		return
	}
	stdin, err := base64.StdEncoding.Strict().DecodeString(request.StdinBase64)
	request.StdinBase64 = ""
	if err != nil || len(stdin) > controllerCommandMaxStdinBytes {
		clear(stdin)
		writeControllerError(w, http.StatusBadRequest, "invalid_stdin", fmt.Sprintf("stdinBase64 must be standard base64 of at most %d bytes", controllerCommandMaxStdinBytes))
		return
	}
	defer clear(stdin)
	timeout := s.opts.CommandTimeout
	if request.TimeoutSeconds != 0 {
		requested := time.Duration(request.TimeoutSeconds) * time.Second
		if request.TimeoutSeconds < 1 || requested > s.opts.CommandTimeout {
			writeControllerError(w, http.StatusBadRequest, "invalid_timeout", fmt.Sprintf("timeoutSeconds must be between 1 and %d", int(s.opts.CommandTimeout/time.Second)))
			return
		}
		timeout = requested
	}

	record, ok := s.workspace(id)
	if !ok {
		writeControllerError(w, http.StatusNotFound, "workspace_not_found", "workspace not found")
		return
	}
	if !record.Request.Capabilities.Commands {
		writeControllerError(w, http.StatusConflict, "commands_not_requested", "workspace was not created with the commands capability")
		return
	}
	s.mu.Lock()
	ready := s.commandWorkspaceReadyLocked(record, record.LeaseID) && IsCanonicalLeaseID(record.LeaseID)
	s.mu.Unlock()
	if !ready {
		writeControllerError(w, http.StatusConflict, "workspace_not_ready", "workspace must be ready, unexpired and not cleaning up")
		return
	}
	if !s.ensureDurableState(id) {
		s.scheduleReconcile(id)
		writeControllerError(w, http.StatusServiceUnavailable, "state_durability_pending", "controller state durability is pending")
		return
	}
	// A command never outlives its workspace's lease.
	if expiresAt, ok := parseLeaseLabelTime(controllerEffectiveExpiry(record, "")); ok {
		if remaining := expiresAt.Sub(s.now()); remaining < timeout {
			timeout = max(remaining, time.Nanosecond)
		}
	}
	commandCtx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	slot := s.workspaceCommandSlot(id)
	select {
	case slot <- struct{}{}:
		defer func() { <-slot }()
	default:
		writeControllerError(w, http.StatusConflict, "command_running", "another command is running in this workspace")
		return
	}
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-commandCtx.Done():
		writeControllerError(w, http.StatusServiceUnavailable, "controller_busy", "controller lifecycle capacity is busy")
		return
	}
	leaseID := record.LeaseID
	effectCtx, effect, beginErr := s.beginSideEffect(commandCtx, id, false, true, true, func(candidate controllerWorkspaceRecord) bool {
		return s.commandWorkspaceReadyLocked(candidate, leaseID)
	})
	if beginErr != nil {
		if errors.Is(beginErr, errControllerStateDurabilityPending) {
			s.scheduleReconcile(id)
			writeControllerError(w, http.StatusServiceUnavailable, "state_durability_pending", "controller state durability is pending")
			return
		}
		writeControllerError(w, http.StatusConflict, "workspace_not_ready", "workspace lifecycle changed before the command started")
		return
	}
	started := s.now()
	stdout := prefixbuffer.NewLimited(controllerCommandMaxOutputBytes)
	stderr := prefixbuffer.NewLimited(controllerCommandMaxOutputBytes)
	exitCode, runErr := runner.RunWorkspaceCommand(effectCtx, leaseID, controllerRequestForRecord(record), argv, stdin, &stdout, &stderr)
	clear(stdin)
	effectCause := context.Cause(effectCtx)
	s.finishSideEffect(effect)
	audit := func(outcome string) {
		if s.log != nil {
			fmt.Fprintf(s.log, "controller command workspace=%s lease=%s command=%s outcome=%s exit=%d stdout_bytes=%d stderr_bytes=%d duration=%s\n",
				id, leaseID, name, outcome, exitCode, len(stdout.Bytes()), len(stderr.Bytes()), s.now().Sub(started).Round(time.Millisecond))
		}
	}
	switch {
	case errors.Is(effectCause, errControllerWorkspaceStopping):
		audit("lifecycle_changed")
		writeControllerError(w, http.StatusConflict, "workspace_not_ready", "workspace lifecycle changed during the command; it was stopped")
		return
	case errors.Is(effectCause, errControllerStateDurabilityPending):
		audit("durability_pending")
		s.scheduleReconcile(id)
		writeControllerError(w, http.StatusServiceUnavailable, "state_durability_pending", "controller state durability became pending during the command; it was stopped")
		return
	case errors.Is(context.Cause(commandCtx), context.DeadlineExceeded):
		audit("timeout")
		writeControllerError(w, http.StatusGatewayTimeout, "command_timeout", "command exceeded its timeout and was stopped")
		return
	case r.Context().Err() != nil:
		audit("canceled")
		return
	case runErr != nil:
		audit("failed")
		writeControllerError(w, http.StatusBadGateway, "command_failed", "could not run the command on the workspace")
		return
	}
	// Serialize the final check with DELETE so a result is never released after
	// a concurrent stop transition.
	s.sideEffectGate.Lock()
	s.mu.Lock()
	final, ok := s.state.Workspaces[id]
	finalReady := ok && s.commandWorkspaceReadyLocked(final, leaseID) && !s.durabilityPending
	if finalReady {
		audit("completed")
		writeControllerJSON(w, http.StatusOK, controllerCommandResponse{
			ExitCode:        exitCode,
			StdoutBase64:    base64.StdEncoding.EncodeToString(stdout.Bytes()),
			StderrBase64:    base64.StdEncoding.EncodeToString(stderr.Bytes()),
			StdoutTruncated: stdout.Exceeded(),
			StderrTruncated: stderr.Exceeded(),
		})
	}
	s.mu.Unlock()
	s.sideEffectGate.Unlock()
	if !finalReady {
		audit("lifecycle_changed")
		writeControllerError(w, http.StatusConflict, "workspace_not_ready", "workspace lifecycle changed before the command result was released")
	}
}
