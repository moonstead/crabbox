package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const execTestSecret = "enrolment-secret-5f2c9e"

type execTestLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *execTestLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *execTestLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

type fakeExecCall struct {
	leaseID        string
	registrationID string
	argv           []string
	stdin          []byte
}

type fakeExecControllerRunner struct {
	*fakeControllerWorkspaceRunner
	execSupported bool
	execMu        sync.Mutex
	execCalls     []fakeExecCall
	execStarted   chan struct{}
	execBlock     chan struct{}
	execCanceled  chan error
	execExit      int
	execErr       error
	execStdout    []byte
	execStderr    []byte
}

func newFakeExecControllerRunner() *fakeExecControllerRunner {
	return &fakeExecControllerRunner{fakeControllerWorkspaceRunner: newFakeControllerWorkspaceRunner(), execSupported: true}
}

func (r *fakeExecControllerRunner) ExecSupported(context.Context) (bool, error) {
	return r.execSupported, nil
}

func (r *fakeExecControllerRunner) ExecWorkspaceCommand(ctx context.Context, request controllerWorkspaceRequest, command controllerExecCommand, stdout, stderr io.Writer) (int, error) {
	r.execMu.Lock()
	r.execCalls = append(r.execCalls, fakeExecCall{
		leaseID: request.ProviderLeaseID, registrationID: command.RegistrationID,
		argv: append([]string(nil), command.Argv...), stdin: append([]byte(nil), command.Stdin...),
	})
	started, block, canceled := r.execStarted, r.execBlock, r.execCanceled
	exit, err, out, errOut := r.execExit, r.execErr, r.execStdout, r.execStderr
	r.execMu.Unlock()
	// Output is produced before any lifecycle change so tests can prove it is
	// withheld rather than merely absent.
	_, _ = stdout.Write(out)
	_, _ = stderr.Write(errOut)
	if started != nil {
		started <- struct{}{}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			if canceled != nil {
				canceled <- context.Cause(ctx)
			}
			return 0, context.Cause(ctx)
		}
	}
	return exit, err
}

func (r *fakeExecControllerRunner) calls() []fakeExecCall {
	r.execMu.Lock()
	defer r.execMu.Unlock()
	return append([]fakeExecCall(nil), r.execCalls...)
}

func testExecControllerOptions(t *testing.T, maxConcurrent int) controllerServiceOptions {
	return controllerServiceOptions{
		StateFile:              filepath.Join(t.TempDir(), "state", "controller.json"),
		MaxConcurrent:          maxConcurrent,
		Allowed:                controllerCapabilities{Desktop: true},
		CreateTimeout:          5 * time.Second,
		InspectTimeout:         time.Second,
		StopTimeout:            time.Second,
		ConnectionTimeout:      time.Second,
		RetryDelay:             10 * time.Millisecond,
		ReadyReconcileInterval: time.Hour,
		ExecAllow:              [][]string{{"sh", "-c"}, {"sh", "-s", "--"}},
		ExecMaxTimeout:         30 * time.Second,
	}
}

func testExecControllerService(t *testing.T, runner controllerWorkspaceRunner, maxConcurrent int) (*controllerService, *execTestLog, context.CancelFunc) {
	t.Helper()
	log := &execTestLog{}
	ctx, cancel := context.WithCancel(context.Background())
	service, err := newControllerService(ctx, testExecControllerOptions(t, maxConcurrent), runner, "test-token", log)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	return service, log, func() {
		cancel()
		service.waitForShutdown()
	}
}

func createExecTestWorkspace(t *testing.T, service *controllerService, id string, request controllerWorkspaceRequest) controllerWorkspaceRecord {
	t.Helper()
	request.ID = id
	created := controllerHTTP(service, http.MethodPost, "/v1/workspaces", "test-token", request)
	if created.Code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	record := waitControllerWorkspaceStatus(t, service, id, "ready")
	waitControllerWorkspaceInactive(t, service, id)
	return record
}

func execTestBody(leaseID string, argv []string, stdin string) map[string]any {
	return map[string]any{
		"argv":        argv,
		"stdinBase64": base64.StdEncoding.EncodeToString([]byte(stdin)),
		"timeoutMs":   10_000,
		"leaseId":     leaseID,
	}
}

func execHTTP(service *controllerService, id string, body any) *httptest.ResponseRecorder {
	return controllerHTTP(service, http.MethodPost, "/v1/workspaces/"+id+"/exec", "test-token", body)
}

func execRawHTTP(service *controllerService, id, contentType string, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/workspaces/"+id+"/exec", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer test-token")
	request.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()
	service.ServeHTTP(recorder, request)
	return recorder
}

func controllerErrorCode(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body %q: %v", recorder.Body.String(), err)
	}
	return body.Error.Code
}

func TestControllerExecRouteDoesNotExistWithoutOperatorOptIn(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, cancel := testControllerService(t, runner, 1)
	defer cancel()
	created := controllerHTTP(service, http.MethodPost, "/v1/workspaces", "test-token", controllerWorkspaceRequest{
		ID: "no-exec-box", Capabilities: controllerCapabilities{Exec: true},
	})
	if created.Code != http.StatusBadRequest || !strings.Contains(created.Body.String(), "exec capability is disabled") {
		t.Fatalf("exec capability without opt-in: status=%d body=%s", created.Code, created.Body.String())
	}
	record := createExecTestWorkspace(t, service, "plain-box", controllerWorkspaceRequest{})
	response := execHTTP(service, "plain-box", execTestBody(record.LeaseID, []string{"sh", "-c", "true"}, execTestSecret))
	if response.Code != http.StatusNotFound || strings.Contains(response.Body.String(), execTestSecret) {
		t.Fatalf("route without opt-in: status=%d body=%s", response.Code, response.Body.String())
	}
	if len(runner.calls()) != 0 {
		t.Fatal("route without opt-in reached the runner")
	}
	get := controllerHTTP(service, http.MethodGet, "/v1/workspaces/plain-box", "test-token", nil)
	if strings.Contains(get.Body.String(), `"exec"`) {
		t.Fatalf("workspace advertised exec without opt-in: %s", get.Body.String())
	}
}

func TestControllerExecStartupRequiresProviderSupport(t *testing.T) {
	for _, test := range []struct {
		name   string
		runner controllerWorkspaceRunner
		want   string
	}{
		{name: "provider lacks claim-exec", runner: func() controllerWorkspaceRunner {
			runner := newFakeExecControllerRunner()
			runner.execSupported = false
			return runner
		}(), want: "does not support claim-fenced execution"},
		{name: "runner cannot execute", runner: newFakeControllerWorkspaceRunner(), want: "cannot execute workspace commands"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			service, err := newControllerService(ctx, testExecControllerOptions(t, 1), test.runner, "test-token", io.Discard)
			if err == nil {
				service.waitForShutdown()
				t.Fatal("adapter started with exec on an unsupported route")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestControllerExecRequiresWorkspaceCapability(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, _, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	record := createExecTestWorkspace(t, service, "no-capability-box", controllerWorkspaceRequest{})
	response := execHTTP(service, "no-capability-box", execTestBody(record.LeaseID, []string{"sh", "-c", "true"}, ""))
	if response.Code != http.StatusForbidden || controllerErrorCode(t, response) != "exec_not_enabled" {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(runner.calls()) != 0 {
		t.Fatal("workspace without exec capability reached the runner")
	}
}

func TestControllerExecEnforcesOperatorArgvPrefixes(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, _, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	record := createExecTestWorkspace(t, service, "policy-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	for _, argv := range [][]string{
		{"bash", "-c", "true"},
		{"sh", "-cx", "true"},
		{"sh"},
		{"sh", "-s"},
		{"/bin/sh", "-c", "true"},
	} {
		response := execHTTP(service, "policy-box", execTestBody(record.LeaseID, argv, ""))
		if response.Code != http.StatusForbidden || controllerErrorCode(t, response) != "exec_argv_not_allowed" {
			t.Fatalf("argv=%q status=%d body=%s", argv, response.Code, response.Body.String())
		}
	}
	if len(runner.calls()) != 0 {
		t.Fatal("unauthorised argv reached the runner")
	}
	for _, argv := range [][]string{{"sh", "-c", "true"}, {"sh", "-s", "--", "--start", "--host-id", "host-1"}} {
		response := execHTTP(service, "policy-box", execTestBody(record.LeaseID, argv, ""))
		if response.Code != http.StatusOK {
			t.Fatalf("argv=%q status=%d body=%s", argv, response.Code, response.Body.String())
		}
	}
}

func TestControllerExecRejectsStaleOrForeignLeaseBeforeExecution(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, log, cancel := testExecControllerService(t, runner, 2)
	defer cancel()
	first := createExecTestWorkspace(t, service, "first-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	second := createExecTestWorkspace(t, service, "second-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	if first.LeaseID == second.LeaseID {
		t.Fatal("fixture workspaces share a lease")
	}
	// Authorisation for the first workspace's lease cannot run on the second.
	response := execHTTP(service, "second-box", execTestBody(first.LeaseID, []string{"sh", "-c", "true"}, execTestSecret))
	if response.Code != http.StatusConflict || controllerErrorCode(t, response) != "workspace_generation_mismatch" {
		t.Fatalf("foreign lease status=%d body=%s", response.Code, response.Body.String())
	}
	response = execHTTP(service, "first-box", execTestBody("cbx_000000000000", []string{"sh", "-c", "true"}, execTestSecret))
	if response.Code != http.StatusConflict || controllerErrorCode(t, response) != "workspace_generation_mismatch" {
		t.Fatalf("stale lease status=%d body=%s", response.Code, response.Body.String())
	}
	response = execHTTP(service, "missing-box", execTestBody(first.LeaseID, []string{"sh", "-c", "true"}, execTestSecret))
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing workspace status=%d", response.Code)
	}
	if len(runner.calls()) != 0 {
		t.Fatal("stale or foreign lease reached the runner")
	}
	runner.execErr = errControllerExecRegistration
	body := execTestBody(first.LeaseID, []string{"sh", "-c", "true"}, execTestSecret)
	body["registrationId"] = "reg-stale"
	response = execHTTP(service, "first-box", body)
	if response.Code != http.StatusConflict || controllerErrorCode(t, response) != "workspace_generation_mismatch" {
		t.Fatalf("stale registration status=%d body=%s", response.Code, response.Body.String())
	}
	if calls := runner.calls(); len(calls) != 1 || calls[0].registrationID != "reg-stale" || calls[0].leaseID != first.LeaseID {
		t.Fatalf("registration was not passed to the claim fence: %+v", calls)
	}
	if strings.Contains(log.String(), execTestSecret) {
		t.Fatal("stdin reached the adapter log")
	}
}

func TestControllerExecBoundsRequestsWithoutEchoingInput(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, log, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	record := createExecTestWorkspace(t, service, "bounds-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	valid := func() map[string]any {
		return execTestBody(record.LeaseID, []string{"sh", "-c", "true"}, execTestSecret)
	}
	for _, test := range []struct {
		name   string
		body   func() []byte
		status int
		code   string
	}{
		{name: "stdin too large", body: func() []byte {
			body := valid()
			body["stdinBase64"] = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("s"), controllerExecMaxStdinBytes+1))
			data, _ := json.Marshal(body)
			return data
		}, status: http.StatusRequestEntityTooLarge, code: "stdin_too_large"},
		{name: "body too large", body: func() []byte {
			body := valid()
			body["argv"] = []string{"sh", "-c", strings.Repeat("x", controllerExecMaxBodyBytes)}
			data, _ := json.Marshal(body)
			return data
		}, status: http.StatusRequestEntityTooLarge, code: "request_too_large"},
		{name: "argv too large", body: func() []byte {
			body := valid()
			body["argv"] = []string{"sh", "-c", strings.Repeat("x", controllerExecMaxArgvBytes)}
			data, _ := json.Marshal(body)
			return data
		}, status: http.StatusBadRequest, code: "invalid_argv"},
		{name: "NUL in argv", body: func() []byte {
			body := valid()
			body["argv"] = []string{"sh", "-c", "a\x00b"}
			data, _ := json.Marshal(body)
			return data
		}, status: http.StatusBadRequest, code: "invalid_argv"},
		{name: "timeout above adapter maximum", body: func() []byte {
			body := valid()
			body["timeoutMs"] = 31_000
			data, _ := json.Marshal(body)
			return data
		}, status: http.StatusBadRequest, code: "invalid_timeout"},
		{name: "timeout missing", body: func() []byte {
			body := valid()
			delete(body, "timeoutMs")
			data, _ := json.Marshal(body)
			return data
		}, status: http.StatusBadRequest, code: "invalid_timeout"},
		{name: "unknown field quoting secret", body: func() []byte {
			body := valid()
			body["env"] = map[string]string{"TOKEN": execTestSecret}
			data, _ := json.Marshal(body)
			return data
		}, status: http.StatusBadRequest, code: "invalid_request"},
		{name: "malformed JSON after secret", body: func() []byte {
			return []byte(`{"stdinBase64":"` + base64.StdEncoding.EncodeToString([]byte(execTestSecret)) + `","argv":[` + execTestSecret)
		}, status: http.StatusBadRequest, code: "invalid_request"},
		{name: "non-base64 stdin", body: func() []byte {
			body := valid()
			body["stdinBase64"] = execTestSecret + "!"
			data, _ := json.Marshal(body)
			return data
		}, status: http.StatusBadRequest, code: "invalid_stdin"},
		{name: "invalid lease", body: func() []byte {
			body := valid()
			body["leaseId"] = "not-a-lease"
			data, _ := json.Marshal(body)
			return data
		}, status: http.StatusBadRequest, code: "invalid_lease_id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := execRawHTTP(service, "bounds-box", "application/json", test.body())
			if response.Code != test.status || controllerErrorCode(t, response) != test.code {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), execTestSecret) {
				t.Fatalf("response echoed private input: %s", response.Body.String())
			}
		})
	}
	response := execRawHTTP(service, "bounds-box", "text/plain", []byte("{}"))
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("content type status=%d", response.Code)
	}
	if len(runner.calls()) != 0 {
		t.Fatal("rejected request reached the runner")
	}
	if strings.Contains(log.String(), execTestSecret) {
		t.Fatal("private input reached the adapter log")
	}
}

func TestControllerExecKeepsStdinPrivateAndReturnsBoundedTail(t *testing.T) {
	runner := newFakeExecControllerRunner()
	runner.execExit = 3
	runner.execStdout = append(bytes.Repeat([]byte("a"), 40<<10), []byte("stdout-end")...)
	runner.execStderr = []byte("failure detail\n")
	service, log, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	record := createExecTestWorkspace(t, service, "secret-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	argv := []string{"sh", "-c", "cat >/dev/null", "bb-machine-install", "https://bb.example.test/install.sh"}
	stdin := execTestSecret + strings.Repeat("-", 40_678)
	response := execHTTP(service, "secret-box", execTestBody(record.LeaseID, argv, stdin))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var result controllerExecResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	stdout, _ := base64.StdEncoding.DecodeString(result.StdoutBase64)
	stderr, _ := base64.StdEncoding.DecodeString(result.StderrBase64)
	if result.ExitCode != 3 || len(stdout) != controllerExecMaxOutputBytes || !bytes.HasSuffix(stdout, []byte("stdout-end")) ||
		!result.StdoutTruncated || result.StdoutBytes != int64(len(runner.execStdout)) ||
		string(stderr) != "failure detail\n" || result.StderrTruncated || result.StderrBytes != 15 {
		t.Fatalf("result exit=%d stdout=%d truncated=%t total=%d stderr=%q", result.ExitCode, len(stdout), result.StdoutTruncated, result.StdoutBytes, stderr)
	}
	if response.Body.Len() >= adapterRelayMaxBodyBytes {
		t.Fatalf("response of %d bytes cannot cross the relay", response.Body.Len())
	}
	calls := runner.calls()
	if len(calls) != 1 || string(calls[0].stdin) != stdin || calls[0].leaseID != record.LeaseID || strings.Join(calls[0].argv, "\x00") != strings.Join(argv, "\x00") {
		t.Fatalf("runner call=%+v", calls)
	}
	if strings.Contains(response.Body.String(), execTestSecret) {
		t.Fatal("stdin echoed in response")
	}
	state, err := os.ReadFile(service.opts.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	audit := log.String()
	if strings.Contains(string(state), execTestSecret) || strings.Contains(audit, execTestSecret) || strings.Contains(audit, "bb-machine-install") {
		t.Fatalf("private input or argv persisted or logged: audit=%q", audit)
	}
	if !strings.Contains(audit, "controller exec workspace=secret-box lease="+record.LeaseID) ||
		!strings.Contains(audit, "outcome=completed exit=3") || !strings.Contains(audit, fmt.Sprintf("stdin_bytes=%d", len(stdin))) ||
		!strings.Contains(audit, "argv_sha256="+controllerExecArgvDigest(argv)) {
		t.Fatalf("audit=%q", audit)
	}
	get := controllerHTTP(service, http.MethodGet, "/v1/workspaces/secret-box", "test-token", nil)
	if !strings.Contains(get.Body.String(), `"exec":true`) {
		t.Fatalf("workspace response did not advertise exec: %s", get.Body.String())
	}
}

func TestControllerExecSetupFailureWithholdsDiagnostics(t *testing.T) {
	runner := newFakeExecControllerRunner()
	runner.execErr = errControllerExecSetup
	runner.execStderr = []byte("provider diagnostic")
	service, _, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	record := createExecTestWorkspace(t, service, "setup-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	response := execHTTP(service, "setup-box", execTestBody(record.LeaseID, []string{"sh", "-c", "true"}, ""))
	if response.Code != http.StatusBadGateway || controllerErrorCode(t, response) != "exec_unavailable" || strings.Contains(response.Body.String(), "provider diagnostic") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

// execDuringLifecycleChange starts a blocking command, applies change and
// returns the response once the command has been cancelled.
func execDuringLifecycleChange(t *testing.T, service *controllerService, runner *fakeExecControllerRunner, id, leaseID string, timeoutMs int, change func()) (*httptest.ResponseRecorder, error) {
	t.Helper()
	runner.execStarted = make(chan struct{}, 1)
	runner.execBlock = make(chan struct{})
	runner.execCanceled = make(chan error, 1)
	runner.execStdout = []byte("stale-output")
	responses := make(chan *httptest.ResponseRecorder, 1)
	body := execTestBody(leaseID, []string{"sh", "-c", "sleep 600"}, execTestSecret)
	body["timeoutMs"] = timeoutMs
	go func() { responses <- execHTTP(service, id, body) }()
	select {
	case <-runner.execStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("command did not start")
	}
	change()
	var cause error
	select {
	case cause = <-runner.execCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle change did not cancel the command")
	}
	select {
	case response := <-responses:
		if strings.Contains(response.Body.String(), "stale-output") || strings.Contains(response.Body.String(), base64.StdEncoding.EncodeToString([]byte("stale-output"))) {
			t.Fatalf("stale output released: %s", response.Body.String())
		}
		return response, cause
	case <-time.After(5 * time.Second):
		t.Fatal("command response did not finish")
	}
	return nil, nil
}

func TestControllerExecDeleteCancelsCommandAndWithholdsOutput(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, log, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	record := createExecTestWorkspace(t, service, "delete-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	response, cause := execDuringLifecycleChange(t, service, runner, "delete-box", record.LeaseID, 10_000, func() {
		if deleted := controllerHTTP(service, http.MethodDelete, "/v1/workspaces/delete-box", "test-token", nil); deleted.Code != http.StatusAccepted {
			t.Fatalf("delete status=%d", deleted.Code)
		}
	})
	if !errors.Is(cause, errControllerWorkspaceStopping) || response.Code != http.StatusConflict || controllerErrorCode(t, response) != "workspace_lifecycle_changed" {
		t.Fatalf("cause=%v status=%d body=%s", cause, response.Code, response.Body.String())
	}
	waitControllerWorkspaceStatus(t, service, "delete-box", "stopped")
	if !strings.Contains(log.String(), "outcome=lifecycle_changed") {
		t.Fatalf("audit=%q", log.String())
	}
	after := execHTTP(service, "delete-box", execTestBody(record.LeaseID, []string{"sh", "-c", "true"}, ""))
	if after.Code != http.StatusConflict || controllerErrorCode(t, after) != "workspace_not_ready" {
		t.Fatalf("exec after delete status=%d body=%s", after.Code, after.Body.String())
	}
}

func TestControllerExecExpiryCapsAndCancelsCommand(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, _, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	base := time.Now().UTC().Truncate(time.Second)
	service.mu.Lock()
	service.now = func() time.Time { return base }
	service.mu.Unlock()
	record := createExecTestWorkspace(t, service, "expiry-box", controllerWorkspaceRequest{TTLSeconds: 60, Capabilities: controllerCapabilities{Exec: true}})
	// 300ms of life remain: a 30-second command must end at the lease expiry.
	nearExpiry := base.Add(60*time.Second - 300*time.Millisecond)
	tick := time.Now()
	service.mu.Lock()
	service.now = func() time.Time { return nearExpiry.Add(time.Since(tick)) }
	service.mu.Unlock()
	response, cause := execDuringLifecycleChange(t, service, runner, "expiry-box", record.LeaseID, 30_000, func() {})
	if !errors.Is(cause, context.DeadlineExceeded) || response.Code != http.StatusConflict || controllerErrorCode(t, response) != "workspace_expired" {
		t.Fatalf("cause=%v status=%d body=%s", cause, response.Code, response.Body.String())
	}
}

func TestControllerExecDurabilityLossCancelsCommand(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, _, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	record := createExecTestWorkspace(t, service, "durability-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	dir := filepath.Dir(service.opts.StateFile)
	response, cause := execDuringLifecycleChange(t, service, runner, "durability-box", record.LeaseID, 10_000, func() {
		setControllerStateSaver(service, func(path string, state controllerState) error {
			return saveControllerStateWithDirectorySync(path, state, func(syncPath string) error {
				if filepath.Clean(syncPath) == filepath.Clean(dir) {
					return errors.New("persistent exec sync failure")
				}
				return nil
			})
		})
		_ = service.updateRecord("durability-box", func(current *controllerWorkspaceRecord) bool {
			current.Message = "ready state refresh"
			return true
		})
	})
	if !errors.Is(cause, errControllerStateDurabilityPending) || response.Code != http.StatusServiceUnavailable {
		t.Fatalf("cause=%v status=%d body=%s", cause, response.Code, response.Body.String())
	}
}

func TestControllerExecFailedStoppingWriteCancelsCommand(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, _, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	record := createExecTestWorkspace(t, service, "revoked-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	response, cause := execDuringLifecycleChange(t, service, runner, "revoked-box", record.LeaseID, 10_000, func() {
		current, _ := service.workspace("revoked-box")
		go service.revokeReadyDesktopAfterTransitionFailure(current, current.ExpiresAt, "expired", "workspace expired")
	})
	if !errors.Is(cause, errControllerWorkspaceStopping) || response.Code != http.StatusConflict {
		t.Fatalf("cause=%v status=%d body=%s", cause, response.Code, response.Body.String())
	}
}

func TestControllerExecTimeoutAndClientCancellation(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, log, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	record := createExecTestWorkspace(t, service, "timeout-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	response, cause := execDuringLifecycleChange(t, service, runner, "timeout-box", record.LeaseID, 1_000, func() {})
	if !errors.Is(cause, context.DeadlineExceeded) || response.Code != http.StatusGatewayTimeout || controllerErrorCode(t, response) != "exec_timeout" {
		t.Fatalf("timeout cause=%v status=%d body=%s", cause, response.Code, response.Body.String())
	}

	runner.execStarted = make(chan struct{}, 1)
	runner.execBlock = make(chan struct{})
	runner.execCanceled = make(chan error, 1)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	data, _ := json.Marshal(execTestBody(record.LeaseID, []string{"sh", "-c", "sleep 600"}, ""))
	request := httptest.NewRequestWithContext(requestCtx, http.MethodPost, "/v1/workspaces/timeout-box/exec", bytes.NewReader(data))
	request.Header.Set("Authorization", "Bearer test-token")
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { service.ServeHTTP(recorder, request); close(done) }()
	<-runner.execStarted
	cancelRequest()
	select {
	case <-runner.execCanceled:
	case <-time.After(3 * time.Second):
		t.Fatal("client cancellation did not reach the command")
	}
	<-done
	if recorder.Body.Len() != 0 {
		t.Fatalf("canceled request received a body: %s", recorder.Body.String())
	}
	if !strings.Contains(log.String(), "outcome=timeout") || !strings.Contains(log.String(), "outcome=canceled") {
		t.Fatalf("audit=%q", log.String())
	}
}

func TestControllerExecSingleFlightDoesNotHoldLifecycleCapacity(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, _, cancel := testExecControllerService(t, runner, 1)
	defer cancel()
	busy := createExecTestWorkspace(t, service, "busy-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	other := createExecTestWorkspace(t, service, "other-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	runner.execStarted = make(chan struct{}, 1)
	runner.execBlock = make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		responses <- execHTTP(service, "busy-box", execTestBody(busy.LeaseID, []string{"sh", "-c", "sleep 600"}, ""))
	}()
	<-runner.execStarted
	second := execHTTP(service, "busy-box", execTestBody(busy.LeaseID, []string{"sh", "-c", "true"}, ""))
	if second.Code != http.StatusConflict || controllerErrorCode(t, second) != "exec_busy" {
		t.Fatalf("second command status=%d body=%s", second.Code, second.Body.String())
	}
	capacity := execHTTP(service, "other-box", execTestBody(other.LeaseID, []string{"sh", "-c", "true"}, ""))
	if capacity.Code != http.StatusTooManyRequests || controllerErrorCode(t, capacity) != "exec_capacity" {
		t.Fatalf("capacity status=%d body=%s", capacity.Code, capacity.Body.String())
	}
	// A running command must not block lifecycle work that could cancel it.
	if deleted := controllerHTTP(service, http.MethodDelete, "/v1/workspaces/other-box", "test-token", nil); deleted.Code != http.StatusAccepted {
		t.Fatalf("delete status=%d", deleted.Code)
	}
	waitControllerWorkspaceStatus(t, service, "other-box", "stopped")
	close(runner.execBlock)
	if response := <-responses; response.Code != http.StatusOK {
		t.Fatalf("first command status=%d body=%s", response.Code, response.Body.String())
	}
}

func execRunnerFixture(t *testing.T, script string) (*execControllerWorkspaceRunner, string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "crabbox")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\n"+script), 0o700); err != nil {
		t.Fatal(err)
	}
	return &execControllerWorkspaceRunner{opts: execControllerRunnerOptions{
		Binary: binary, Provider: "proxmox", StateFile: filepath.Join(dir, "state.json"),
	}}, dir
}

func execRunnerRequest() controllerWorkspaceRequest {
	return controllerWorkspaceRequest{ID: "runner-box", ProviderRoute: "proxmox", ProviderScope: "scope", ProviderLeaseID: "cbx_abcdef123456"}
}

func TestControllerExecRunnerPassesStdinOnlyThroughPrivatePipe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("controller host is unsupported on Windows")
	}
	runner, dir := execRunnerFixture(t, `printf '%s\n' "$@" > "$0.args"
/usr/bin/env > "$0.env"
cat > "$0.stdin"
printf '{"event":"started"}\n' >&5
printf out
printf err >&2
exit 7
`)
	var stdout, stderr bytes.Buffer
	exit, err := runner.ExecWorkspaceCommand(context.Background(), execRunnerRequest(), controllerExecCommand{
		Argv: []string{"sh", "-c", "cat"}, Stdin: []byte(execTestSecret), RegistrationID: "reg-1",
	}, &stdout, &stderr)
	if err != nil || exit != 7 || stdout.String() != "out" || stderr.String() != "err" {
		t.Fatalf("exit=%d err=%v stdout=%q stderr=%q", exit, err, stdout.String(), stderr.String())
	}
	binary := filepath.Join(dir, "crabbox")
	stdin, _ := os.ReadFile(binary + ".stdin")
	args, _ := os.ReadFile(binary + ".args")
	environment, _ := os.ReadFile(binary + ".env")
	if string(stdin) != execTestSecret {
		t.Fatalf("stdin=%q", stdin)
	}
	want := "exec\n--id\ncbx_abcdef123456\n--status-fd\n5\n--terminate-remote-on-disconnect\n--expect-runtime-registration\nreg-1\n--provider\nproxmox\n--\nsh\n-c\ncat\n"
	if string(args) != want {
		t.Fatalf("args=%q", args)
	}
	if strings.Contains(string(args), execTestSecret) || strings.Contains(string(environment), execTestSecret) {
		t.Fatal("stdin entered child argv or environment")
	}
}

func TestControllerExecRunnerClassifiesSetupAndRegistrationFailures(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("controller host is unsupported on Windows")
	}
	runner, _ := execRunnerFixture(t, "printf 'provider diagnostic' >&2\nexit 2\n")
	_, err := runner.ExecWorkspaceCommand(context.Background(), execRunnerRequest(), controllerExecCommand{Argv: []string{"true"}}, io.Discard, io.Discard)
	if !errors.Is(err, errControllerExecSetup) {
		t.Fatalf("setup failure=%v", err)
	}
	runner, _ = execRunnerFixture(t, "printf '{\"event\":\"rejected\",\"reason\":\"registration\"}\\n' >&5\nexit 4\n")
	_, err = runner.ExecWorkspaceCommand(context.Background(), execRunnerRequest(), controllerExecCommand{Argv: []string{"true"}, RegistrationID: "reg-1"}, io.Discard, io.Discard)
	if !errors.Is(err, errControllerExecRegistration) {
		t.Fatalf("registration failure=%v", err)
	}
	runner.opts.StateFile = ""
	_, err = runner.ExecWorkspaceCommand(context.Background(), execRunnerRequest(), controllerExecCommand{Argv: []string{"true"}}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "durable child registry") {
		t.Fatalf("untracked private streams=%v", err)
	}
}

func TestControllerExecRunnerCancellationTerminatesProcessTree(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("controller host is unsupported on Windows")
	}
	runner, dir := execRunnerFixture(t, `printf '{"event":"started"}\n' >&5
sleep 60 &
printf '%s\n' "$!" > "$0.pid"
wait
`)
	pidPath := filepath.Join(dir, "crabbox.pid")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runner.ExecWorkspaceCommand(ctx, execRunnerRequest(), controllerExecCommand{Argv: []string{"true"}, Stdin: []byte(execTestSecret)}, io.Discard, io.Discard)
		done <- err
	}()
	var pid int
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && pid <= 0 {
		if data, err := os.ReadFile(pidPath); err == nil {
			fmt.Sscan(strings.TrimSpace(string(data)), &pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid <= 0 {
		cancel()
		<-done
		t.Fatal("command descendant did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled command did not return")
	}
	if command, alive := LocalProcessCommand(pid); alive && !strings.Contains(strings.ToLower(command), "<defunct>") {
		t.Fatalf("command descendant survived cancellation pid=%d command=%q", pid, command)
	}
}

// A restart must reload exec workspaces whether or not the new process still
// enables exec; the running policy decides admission.
func TestControllerExecWorkspaceSurvivesRestartWithAndWithoutExec(t *testing.T) {
	runner := newFakeExecControllerRunner()
	service, _, cancel := testExecControllerService(t, runner, 1)
	record := createExecTestWorkspace(t, service, "restart-box", controllerWorkspaceRequest{Capabilities: controllerCapabilities{Exec: true}})
	opts := service.opts
	cancel()
	if _, err := loadControllerState(opts.StateFile); err != nil {
		t.Fatalf("state with an exec workspace does not validate: %v", err)
	}
	for _, enabled := range []bool{true, false} {
		restartOpts := opts
		if !enabled {
			restartOpts.ExecAllow = nil
		}
		ctx, stop := context.WithCancel(context.Background())
		restarted, err := newControllerService(ctx, restartOpts, runner, "test-token", io.Discard)
		if err != nil {
			stop()
			t.Fatalf("restart with exec=%t: %v", enabled, err)
		}
		response := execHTTP(restarted, "restart-box", execTestBody(record.LeaseID, []string{"sh", "-c", "true"}, ""))
		get := controllerHTTP(restarted, http.MethodGet, "/v1/workspaces/restart-box", "test-token", nil)
		stop()
		restarted.waitForShutdown()
		if enabled && (response.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"exec":true`)) {
			t.Fatalf("exec after restart status=%d body=%s workspace=%s", response.Code, response.Body.String(), get.Body.String())
		}
		if !enabled && (response.Code != http.StatusNotFound || strings.Contains(get.Body.String(), `"exec"`)) {
			t.Fatalf("exec after restart without opt-in status=%d workspace=%s", response.Code, get.Body.String())
		}
	}
}
