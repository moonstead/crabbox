package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const commandTestSecret = "one-time-enrolment-secret"

var commandTestArgv = []string{"/usr/local/bin/enrol", "--from-stdin"}

type fakeCommandCall struct {
	leaseID string
	argv    []string
	stdin   string
}

type fakeCommandRunner struct {
	*fakeControllerWorkspaceRunner
	mu          sync.Mutex
	calls       []fakeCommandCall
	started     chan context.Context
	unsupported bool
	run         func(context.Context, io.Writer, io.Writer) (int, error)
}

func (r *fakeCommandRunner) RunWorkspaceCommand(ctx context.Context, leaseID string, _ controllerWorkspaceRequest, argv []string, stdin []byte, stdout, stderr io.Writer) (int, error) {
	r.mu.Lock()
	r.calls = append(r.calls, fakeCommandCall{leaseID: leaseID, argv: append([]string(nil), argv...), stdin: string(stdin)})
	r.mu.Unlock()
	if r.started != nil {
		r.started <- ctx
	}
	if r.run != nil {
		return r.run(ctx, stdout, stderr)
	}
	_, _ = io.WriteString(stdout, "enrolled\n")
	_, _ = io.WriteString(stderr, "warning\n")
	return 7, nil
}

func (r *fakeCommandRunner) CommandExecutionSupported(context.Context) (bool, error) {
	return !r.unsupported, nil
}

func (r *fakeCommandRunner) commandCalls() []fakeCommandCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]fakeCommandCall(nil), r.calls...)
}

func commandTestService(t *testing.T, runner controllerWorkspaceRunner, commands map[string][]string) (*controllerService, *bytes.Buffer) {
	t.Helper()
	log := &bytes.Buffer{}
	ctx, cancel := context.WithCancel(context.Background())
	service, err := newControllerService(ctx, controllerServiceOptions{
		StateFile:     filepath.Join(t.TempDir(), "state", "controller.json"),
		MaxConcurrent: 2, Profile: "public-desktop",
		CreateTimeout: 5 * time.Second, InspectTimeout: time.Second, StopTimeout: time.Second,
		ConnectionTimeout: time.Second, RetryDelay: 10 * time.Millisecond, ReadyReconcileInterval: time.Hour,
		Commands: commands, CommandTimeout: 2 * time.Second,
	}, runner, "test-token", log)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		service.waitForShutdown()
	})
	return service, log
}

func createCommandWorkspace(t *testing.T, service *controllerService, id string, commands bool) controllerWorkspaceRecord {
	t.Helper()
	created := controllerHTTP(service, http.MethodPost, "/v1/workspaces", "test-token", controllerWorkspaceRequest{ID: id, Capabilities: controllerCapabilities{Commands: commands}})
	if created.Code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	return waitControllerWorkspaceStatus(t, service, id, "ready")
}

func commandHTTP(service *controllerService, path string, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer test-token")
	recorder := httptest.NewRecorder()
	service.ServeHTTP(recorder, request)
	return recorder
}

func commandBody(stdin string, timeoutSeconds int) string {
	data, _ := json.Marshal(controllerCommandRequest{StdinBase64: base64.StdEncoding.EncodeToString([]byte(stdin)), TimeoutSeconds: timeoutSeconds})
	return string(data)
}

func TestControllerCommandsAreDisabledByDefault(t *testing.T) {
	runner := &fakeCommandRunner{fakeControllerWorkspaceRunner: newFakeControllerWorkspaceRunner()}
	service, _ := commandTestService(t, runner, nil)
	createCommandWorkspace(t, service, "plain-box", false)
	for _, response := range []*httptest.ResponseRecorder{
		commandHTTP(service, "/v1/workspaces/plain-box/commands/enrol", commandBody("x", 0)),
		controllerHTTP(service, http.MethodGet, "/v1/commands", "test-token", nil),
	} {
		if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), `"code":"not_found"`) {
			t.Fatalf("disabled commands route status=%d body=%s", response.Code, response.Body.String())
		}
	}
	rejected := controllerHTTP(service, http.MethodPost, "/v1/workspaces", "test-token", controllerWorkspaceRequest{ID: "command-box", Capabilities: controllerCapabilities{Commands: true}})
	if rejected.Code != http.StatusBadRequest || !strings.Contains(rejected.Body.String(), "commands capability is disabled") {
		t.Fatalf("commands capability status=%d body=%s", rejected.Code, rejected.Body.String())
	}
	if len(runner.commandCalls()) != 0 {
		t.Fatal("disabled command ran")
	}
}

func TestControllerCommandRunsOperatorArgvWithPrivateStdin(t *testing.T) {
	runner := &fakeCommandRunner{fakeControllerWorkspaceRunner: newFakeControllerWorkspaceRunner()}
	service, log := commandTestService(t, runner, map[string][]string{"enrol": commandTestArgv})
	record := createCommandWorkspace(t, service, "command-box", true)

	listed := controllerHTTP(service, http.MethodGet, "/v1/commands", "test-token", nil)
	if listed.Code != http.StatusOK || strings.Contains(listed.Body.String(), commandTestArgv[0]) {
		t.Fatalf("command list status=%d body=%s", listed.Code, listed.Body.String())
	}
	var advertised struct {
		Version  int      `json:"version"`
		Commands []string `json:"commands"`
	}
	if err := json.Unmarshal(listed.Body.Bytes(), &advertised); err != nil || advertised.Version != 1 || !reflect.DeepEqual(advertised.Commands, []string{"enrol"}) {
		t.Fatalf("advertised=%+v err=%v", advertised, err)
	}

	response := commandHTTP(service, "/v1/workspaces/command-box/commands/enrol", commandBody(commandTestSecret, 1))
	if response.Code != http.StatusOK {
		t.Fatalf("command status=%d body=%s", response.Code, response.Body.String())
	}
	var result controllerCommandResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	stdout, _ := base64.StdEncoding.DecodeString(result.StdoutBase64)
	stderr, _ := base64.StdEncoding.DecodeString(result.StderrBase64)
	if result.ExitCode != 7 || string(stdout) != "enrolled\n" || string(stderr) != "warning\n" || result.StdoutTruncated || result.StderrTruncated {
		t.Fatalf("result=%+v stdout=%q stderr=%q", result, stdout, stderr)
	}
	calls := runner.commandCalls()
	if len(calls) != 1 || calls[0].leaseID != record.LeaseID || !reflect.DeepEqual(calls[0].argv, commandTestArgv) || calls[0].stdin != commandTestSecret {
		t.Fatalf("calls=%+v", calls)
	}
	state, err := os.ReadFile(service.opts.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(commandTestSecret))
	for name, text := range map[string]string{"state": string(state), "log": log.String(), "response": response.Body.String()} {
		if strings.Contains(text, commandTestSecret) || strings.Contains(text, encoded) {
			t.Fatalf("stdin reached the %s", name)
		}
	}
	if !strings.Contains(log.String(), "controller command workspace=command-box lease="+record.LeaseID+" command=enrol outcome=completed exit=7") {
		t.Fatalf("audit log=%q", log.String())
	}
}

func TestControllerCommandRejectsUnsafeRequests(t *testing.T) {
	runner := &fakeCommandRunner{fakeControllerWorkspaceRunner: newFakeControllerWorkspaceRunner()}
	service, log := commandTestService(t, runner, map[string][]string{"enrol": commandTestArgv})
	createCommandWorkspace(t, service, "command-box", true)
	createCommandWorkspace(t, service, "plain-box", false)
	oversized := commandBody(strings.Repeat("s", controllerCommandMaxStdinBytes+1), 0)
	for _, tc := range []struct {
		name, path, body string
		status           int
		code             string
	}{
		{"not requested", "/v1/workspaces/plain-box/commands/enrol", commandBody(commandTestSecret, 0), http.StatusConflict, "commands_not_requested"},
		{"unknown command", "/v1/workspaces/command-box/commands/shell", commandBody(commandTestSecret, 0), http.StatusNotFound, "command_not_found"},
		{"unknown workspace", "/v1/workspaces/missing-box/commands/enrol", commandBody(commandTestSecret, 0), http.StatusNotFound, "workspace_not_found"},
		{"invalid base64", "/v1/workspaces/command-box/commands/enrol", `{"stdinBase64":"` + commandTestSecret + `"}`, http.StatusBadRequest, "invalid_stdin"},
		{"oversized stdin", "/v1/workspaces/command-box/commands/enrol", oversized, http.StatusBadRequest, "invalid_stdin"},
		{"caller argv", "/v1/workspaces/command-box/commands/enrol", `{"argv":["/bin/sh"],"stdinBase64":"` + commandTestSecret + `"}`, http.StatusBadRequest, "invalid_request"},
		{"timeout above policy", "/v1/workspaces/command-box/commands/enrol", commandBody(commandTestSecret, 3), http.StatusBadRequest, "invalid_timeout"},
		{"negative timeout", "/v1/workspaces/command-box/commands/enrol", commandBody(commandTestSecret, -1), http.StatusBadRequest, "invalid_timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := commandHTTP(service, tc.path, tc.body)
			if response.Code != tc.status || !strings.Contains(response.Body.String(), `"code":"`+tc.code+`"`) {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), commandTestSecret) {
				t.Fatal("error response echoed stdin")
			}
		})
	}
	wrongType := httptest.NewRequest(http.MethodPost, "/v1/workspaces/command-box/commands/enrol", strings.NewReader(commandBody("x", 0)))
	wrongType.Header.Set("Authorization", "Bearer test-token")
	recorder := httptest.NewRecorder()
	service.ServeHTTP(recorder, wrongType)
	if recorder.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("content type status=%d", recorder.Code)
	}
	if get := controllerHTTP(service, http.MethodGet, "/v1/workspaces/command-box/commands/enrol", "test-token", nil); get.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status=%d", get.Code)
	}
	if unauthorized := controllerHTTP(service, http.MethodPost, "/v1/workspaces/command-box/commands/enrol", "", controllerCommandRequest{}); unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}
	if calls := runner.commandCalls(); len(calls) != 0 || strings.Contains(log.String(), commandTestSecret) {
		t.Fatalf("rejected requests ran commands=%d or logged stdin", len(calls))
	}
}

func TestControllerCommandBoundsOutput(t *testing.T) {
	runner := &fakeCommandRunner{fakeControllerWorkspaceRunner: newFakeControllerWorkspaceRunner()}
	runner.run = func(_ context.Context, stdout, stderr io.Writer) (int, error) {
		_, _ = stdout.Write(bytes.Repeat([]byte("o"), controllerCommandMaxOutputBytes+100))
		_, _ = stderr.Write([]byte("short"))
		return 0, nil
	}
	service, _ := commandTestService(t, runner, map[string][]string{"enrol": commandTestArgv})
	createCommandWorkspace(t, service, "command-box", true)
	response := commandHTTP(service, "/v1/workspaces/command-box/commands/enrol", commandBody("", 0))
	var result controllerCommandResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || response.Code != http.StatusOK {
		t.Fatalf("status=%d err=%v", response.Code, err)
	}
	stdout, _ := base64.StdEncoding.DecodeString(result.StdoutBase64)
	if len(stdout) != controllerCommandMaxOutputBytes || !result.StdoutTruncated || result.StderrTruncated {
		t.Fatalf("stdout=%d truncated=%t/%t", len(stdout), result.StdoutTruncated, result.StderrTruncated)
	}
	if response.Body.Len() > controllerMaxBodyBytes {
		t.Fatalf("response exceeds the relay body bound: %d", response.Body.Len())
	}
}

func blockingCommandRunner() *fakeCommandRunner {
	runner := &fakeCommandRunner{fakeControllerWorkspaceRunner: newFakeControllerWorkspaceRunner(), started: make(chan context.Context, 1)}
	runner.run = func(ctx context.Context, _, _ io.Writer) (int, error) {
		<-ctx.Done()
		return 0, context.Cause(ctx)
	}
	return runner
}

func TestControllerCommandIsFencedByWorkspaceRelease(t *testing.T) {
	runner := blockingCommandRunner()
	service, log := commandTestService(t, runner, map[string][]string{"enrol": commandTestArgv})
	createCommandWorkspace(t, service, "command-box", true)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- commandHTTP(service, "/v1/workspaces/command-box/commands/enrol", commandBody(commandTestSecret, 0))
	}()
	commandCtx := <-runner.started
	if deleted := controllerHTTP(service, http.MethodDelete, "/v1/workspaces/command-box", "test-token", nil); deleted.Code >= 300 {
		t.Fatalf("delete status=%d body=%s", deleted.Code, deleted.Body.String())
	}
	response := <-done
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "lifecycle changed during the command") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if !errors.Is(context.Cause(commandCtx), errControllerWorkspaceStopping) {
		t.Fatalf("command cancellation cause=%v", context.Cause(commandCtx))
	}
	if !strings.Contains(log.String(), "outcome=lifecycle_changed") {
		t.Fatalf("audit log=%q", log.String())
	}
	if again := commandHTTP(service, "/v1/workspaces/command-box/commands/enrol", commandBody(commandTestSecret, 0)); again.Code != http.StatusConflict {
		t.Fatalf("stopping workspace accepted a command: status=%d", again.Code)
	}
}

func TestControllerCommandTimeoutStopsCommand(t *testing.T) {
	runner := blockingCommandRunner()
	service, log := commandTestService(t, runner, map[string][]string{"enrol": commandTestArgv})
	createCommandWorkspace(t, service, "command-box", true)
	started := time.Now()
	response := commandHTTP(service, "/v1/workspaces/command-box/commands/enrol", commandBody("", 1))
	commandCtx := <-runner.started
	if response.Code != http.StatusGatewayTimeout || !errors.Is(context.Cause(commandCtx), context.DeadlineExceeded) || time.Since(started) > 5*time.Second {
		t.Fatalf("status=%d cause=%v elapsed=%s", response.Code, context.Cause(commandCtx), time.Since(started))
	}
	if !strings.Contains(log.String(), "outcome=timeout") {
		t.Fatalf("audit log=%q", log.String())
	}
}

func TestControllerCommandDefinitionParsing(t *testing.T) {
	commands := map[string][]string{}
	if err := parseControllerCommandDefinition(`enrol=["/usr/local/bin/enrol","--from-stdin"]`, commands); err != nil || !reflect.DeepEqual(commands["enrol"], commandTestArgv) {
		t.Fatalf("commands=%v err=%v", commands, err)
	}
	for _, value := range []string{
		`enrol=["/bin/true"]`,
		`Enrol=["/bin/true"]`,
		`shell=["sh","-c","id"]`,
		`empty=[]`,
		`blank=["/bin/echo",""]`,
		`object={"argv":["/bin/true"]}`,
		`two=["/bin/true"] ["/bin/false"]`,
		`noequals`,
	} {
		if err := parseControllerCommandDefinition(value, commands); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestAdapterRelayDoesNotExposeWorkspaceCommands(t *testing.T) {
	body := commandBody(commandTestSecret, 0)
	request := adapterRelayRequest{Type: "request", ID: "relay-1", Method: http.MethodPost, Path: "/v1/workspaces/command-box/commands/enrol", DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), Body: &body}
	if err := validateAdapterRelayRequest(request); err == nil {
		t.Fatal("the coordinator relay admitted a workspace command")
	}
	request.Method, request.Path, request.Body = http.MethodGet, "/v1/commands", nil
	if err := validateAdapterRelayRequest(request); err == nil {
		t.Fatal("the coordinator relay admitted command discovery")
	}
}

func TestControllerCommandDeadlineEndsAtWorkspaceExpiry(t *testing.T) {
	runner := blockingCommandRunner()
	service, _ := commandTestService(t, runner, map[string][]string{"enrol": commandTestArgv})
	record := createCommandWorkspace(t, service, "command-box", true)
	expiresAt := time.Now().Add(1500 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	if err := service.updateRecord(record.Request.ID, func(current *controllerWorkspaceRecord) bool {
		current.ExpiresAt, current.ControllerExpiresAt = expiresAt, expiresAt
		return true
	}); err != nil {
		t.Fatal(err)
	}
	response := commandHTTP(service, "/v1/workspaces/command-box/commands/enrol", commandBody("", 2))
	commandCtx := <-runner.started
	deadline, ok := commandCtx.Deadline()
	if !ok || deadline.After(time.Now().Add(time.Second)) || response.Code != http.StatusGatewayTimeout {
		t.Fatalf("deadline=%s ok=%t status=%d", deadline, ok, response.Code)
	}
}

func TestControllerCommandsRequireProviderExecutionSupport(t *testing.T) {
	for name, runner := range map[string]controllerWorkspaceRunner{
		"unsupported provider":    &fakeCommandRunner{fakeControllerWorkspaceRunner: newFakeControllerWorkspaceRunner(), unsupported: true},
		"runner without commands": newFakeControllerWorkspaceRunner(),
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := newControllerService(ctx, controllerServiceOptions{
				StateFile: filepath.Join(t.TempDir(), "state.json"), MaxConcurrent: 1,
				CreateTimeout: time.Second, InspectTimeout: time.Second, StopTimeout: time.Second, ConnectionTimeout: time.Second,
				Commands: map[string][]string{"enrol": commandTestArgv}, CommandTimeout: time.Second,
			}, runner, "test-token", io.Discard)
			if err == nil {
				t.Fatal("adapter started with commands it cannot execute")
			}
		})
	}
}

func TestControllerCommandRejectsConcurrentCommand(t *testing.T) {
	runner := blockingCommandRunner()
	service, _ := commandTestService(t, runner, map[string][]string{"enrol": commandTestArgv})
	createCommandWorkspace(t, service, "command-box", true)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- commandHTTP(service, "/v1/workspaces/command-box/commands/enrol", commandBody("", 1))
	}()
	<-runner.started
	busy := commandHTTP(service, "/v1/workspaces/command-box/commands/enrol", commandBody("", 1))
	if busy.Code != http.StatusConflict || !strings.Contains(busy.Body.String(), `"code":"command_running"`) {
		t.Fatalf("concurrent command status=%d body=%s", busy.Code, busy.Body.String())
	}
	if first := <-done; first.Code != http.StatusGatewayTimeout {
		t.Fatalf("first command status=%d", first.Code)
	}
	if calls := runner.commandCalls(); len(calls) != 1 {
		t.Fatalf("calls=%d", len(calls))
	}
}
