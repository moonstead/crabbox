package cli

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

func adapterRelayExecTestBody(t *testing.T, fields map[string]any) string {
	t.Helper()
	body := map[string]any{
		"argv":           []string{"sh", "-c", "cat"},
		"stdinBase64":    base64.StdEncoding.EncodeToString([]byte(execTestSecret)),
		"timeoutMs":      5_000,
		"leaseId":        "cbx_abcdef123456",
		"registrationId": "reg-current",
	}
	for key, value := range fields {
		if value == nil {
			delete(body, key)
		} else {
			body[key] = value
		}
	}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestAdapterRelayExecRequiresOptInAndCoordinatorBinding(t *testing.T) {
	valid := adapterRelayExecTestBody(t, nil)
	request := adapterRelayRequest{Type: "request", ID: "exec-1", Method: http.MethodPost, Path: "/v1/workspaces/fleet-a-is-101/exec", DeadlineMS: 4102444800000, Body: &valid}
	if err := validateAdapterRelayRequest(request); err == nil || !strings.Contains(err.Error(), "not enabled") {
		t.Fatalf("exec without connector opt-in=%v", err)
	}
	if err := validateAdapterRelayRequestWithExec(request, true); err != nil {
		t.Fatalf("valid exec rejected: %v", err)
	}
	if adapterRelayRouteAllowed(http.MethodPost, request.Path) {
		t.Fatal("exec joined the ordinary relay surface")
	}
	for name, body := range map[string]*string{
		"missing body":         nil,
		"missing registration": ptr(adapterRelayExecTestBody(t, map[string]any{"registrationId": nil})),
		"missing lease":        ptr(adapterRelayExecTestBody(t, map[string]any{"leaseId": nil})),
		"invalid registration": ptr(adapterRelayExecTestBody(t, map[string]any{"registrationId": "Reg_Current"})),
		"not an object":        ptr(`["` + execTestSecret + `"]`),
		"oversized":            ptr(adapterRelayExecTestBody(t, map[string]any{"stdinBase64": strings.Repeat("A", adapterRelayMaxExecBodyBytes)})),
	} {
		candidate := request
		candidate.Body = body
		err := validateAdapterRelayRequestWithExec(candidate, true)
		if err == nil {
			t.Fatalf("%s accepted", name)
		}
		if strings.Contains(err.Error(), execTestSecret) {
			t.Fatalf("%s error quoted private input: %v", name, err)
		}
	}
	for _, path := range []string{"/v1/workspaces/Bad_ID/exec", "/v1/workspaces/a/exec?x=1", "/v1/workspaces/a/b/exec", "/v1/workspaces/a%2Fb/exec"} {
		if adapterRelayExecPath(http.MethodPost, path) {
			t.Fatalf("path %q accepted", path)
		}
	}
	if adapterRelayExecPath(http.MethodGet, request.Path) {
		t.Fatal("GET exec accepted")
	}
	if got := adapterRelayTimeout(request, time.Minute, 16*time.Minute); got != 16*time.Minute {
		t.Fatalf("exec timeout=%s", got)
	}
}

func ptr(value string) *string { return &value }

type adapterExecRelayFixture struct {
	ticketExec   atomic.Int64
	localCalls   atomic.Int32
	localCancels chan struct{}
	localBodies  chan string
}

// runAdapterExecRelay connects a relay to a scripted coordinator. script
// writes frames and returns the responses it read.
func runAdapterExecRelay(t *testing.T, execTimeout time.Duration, script func(ctx context.Context, conn *websocket.Conn, fixture *adapterExecRelayFixture) error) *adapterExecRelayFixture {
	t.Helper()
	fixture := &adapterExecRelayFixture{localCancels: make(chan struct{}, 4), localBodies: make(chan string, 4)}
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.localCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer local-token" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("local exec headers auth=%q type=%q", r.Header.Get("Authorization"), r.Header.Get("Content-Type"))
		}
		body, _ := io.ReadAll(r.Body)
		fixture.localBodies <- string(body)
		if strings.Contains(string(body), `"sleep 600"`) {
			<-r.Context().Done()
			fixture.localCancels <- struct{}{}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"exitCode":0,"stdoutBase64":"","stderrBase64":""}`)
	}))
	defer local.Close()
	scriptErr := make(chan error, 1)
	coordinator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/adapters/mac-lab/ticket":
			var ticket struct {
				ExecTimeoutMS *int64 `json:"execTimeoutMs"`
			}
			_ = json.NewDecoder(r.Body).Decode(&ticket)
			if ticket.ExecTimeoutMS != nil {
				fixture.ticketExec.Store(*ticket.ExecTimeoutMS)
			}
			_, _ = io.WriteString(w, `{"ticket":"adapter-ticket"}`)
		case "/v1/adapters/mac-lab/agent":
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				scriptErr <- err
				return
			}
			defer conn.Close(websocket.StatusNormalClosure, "test complete")
			scriptErr <- script(r.Context(), conn, fixture)
		default:
			http.NotFound(w, r)
		}
	}))
	defer coordinator.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	execRequestTimeout := time.Duration(0)
	if execTimeout > 0 {
		execRequestTimeout = execTimeout + adapterRelayExecOverhead
	}
	err := connectAdapterRelay(ctx, &CoordinatorClient{BaseURL: coordinator.URL, Token: "coordinator-token", Client: coordinator.Client()},
		"mac-lab", local.URL, "/test/adapter.sock", func() (string, error) { return "local-token", nil },
		local.Client(), 150*time.Second, execRequestTimeout, io.Discard)
	var closeError websocket.CloseError
	if err == nil || !errors.As(err, &closeError) || closeError.Code != websocket.StatusNormalClosure {
		t.Fatalf("relay close error=%v", err)
	}
	if err := <-scriptErr; err != nil {
		t.Fatal(err)
	}
	return fixture
}

func writeRelayFrame(ctx context.Context, conn *websocket.Conn, frame any) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func readRelayResponse(ctx context.Context, conn *websocket.Conn) (adapterRelayResponse, error) {
	var response adapterRelayResponse
	_, data, err := conn.Read(ctx)
	if err != nil {
		return response, err
	}
	return response, json.Unmarshal(data, &response)
}

func TestConnectAdapterRelayExecRelaysAndHonoursCancelFrames(t *testing.T) {
	body := adapterRelayExecTestBody(t, nil)
	blocking := adapterRelayExecTestBody(t, map[string]any{"argv": []string{"sh", "-c", "sleep 600"}})
	fixture := runAdapterExecRelay(t, 15*time.Minute, func(ctx context.Context, conn *websocket.Conn, fixture *adapterExecRelayFixture) error {
		if err := writeRelayFrame(ctx, conn, adapterRelayRequest{Type: "request", ID: "exec-1", Method: http.MethodPost, Path: "/v1/workspaces/fleet-a-is-101/exec", DeadlineMS: 4102444800000, Body: &body}); err != nil {
			return err
		}
		response, err := readRelayResponse(ctx, conn)
		if err != nil {
			return err
		}
		if response.ID != "exec-1" || response.Status != http.StatusOK || !strings.Contains(response.Body, `"exitCode":0`) {
			return errors.New("exec response was not relayed: " + response.Body)
		}
		if first := <-fixture.localBodies; first != body {
			return errors.New("local exec body changed in transit")
		}
		if err := writeRelayFrame(ctx, conn, adapterRelayRequest{Type: "request", ID: "exec-2", Method: http.MethodPost, Path: "/v1/workspaces/fleet-a-is-101/exec", DeadlineMS: 4102444800000, Body: &blocking}); err != nil {
			return err
		}
		// Wait until the local request is running, then withdraw it.
		select {
		case <-fixture.localBodies:
		case <-time.After(3 * time.Second):
			return errors.New("blocking exec did not reach the local adapter")
		}
		if err := writeRelayFrame(ctx, conn, map[string]string{"type": "cancel", "id": "exec-2"}); err != nil {
			return err
		}
		select {
		case <-fixture.localCancels:
		case <-time.After(3 * time.Second):
			return errors.New("cancel frame did not cancel the local exec request")
		}
		response, err = readRelayResponse(ctx, conn)
		if err != nil {
			return err
		}
		if response.ID != "exec-2" || response.Status == http.StatusOK {
			return errors.New("canceled exec produced a successful response")
		}
		return nil
	})
	if got := fixture.ticketExec.Load(); got != (15*time.Minute + adapterRelayExecOverhead + adapterRelayWriteTimeout).Milliseconds() {
		t.Fatalf("ticket exec timeout=%d", got)
	}
}

func TestConnectAdapterRelayWithoutExecOptInRejectsExecFrames(t *testing.T) {
	body := adapterRelayExecTestBody(t, nil)
	fixture := runAdapterExecRelay(t, 0, func(ctx context.Context, conn *websocket.Conn, _ *adapterExecRelayFixture) error {
		if err := writeRelayFrame(ctx, conn, adapterRelayRequest{Type: "request", ID: "exec-1", Method: http.MethodPost, Path: "/v1/workspaces/fleet-a-is-101/exec", DeadlineMS: 4102444800000, Body: &body}); err != nil {
			return err
		}
		response, err := readRelayResponse(ctx, conn)
		if err != nil {
			return err
		}
		if response.ID != "exec-1" || response.Status != http.StatusBadRequest || strings.Contains(response.Body, execTestSecret) {
			return errors.New("exec without opt-in was not rejected: " + response.Body)
		}
		return nil
	})
	if fixture.ticketExec.Load() != 0 {
		t.Fatal("connector advertised exec without opt-in")
	}
	if fixture.localCalls.Load() != 0 {
		t.Fatal("exec without opt-in reached the local adapter")
	}
}
