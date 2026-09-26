package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

func TestDetectWebVNCInputGate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		server func(net.Conn)
		gate   bool
		replay string
	}{
		{"gate greeting", func(c net.Conn) { _, _ = c.Write([]byte(webVNCInputGateGreeting + "rest")) }, true, "rest"},
		{"plain VNC server", func(c net.Conn) { _, _ = c.Write([]byte("RFB 003.008\n")) }, false, "RFB 003.008\n"},
		{"server closes", func(c net.Conn) { _ = c.Close() }, false, ""},
		{"silent server", func(c net.Conn) {}, false, ""},
		{"short greeting", func(c net.Conn) { _, _ = c.Write([]byte("CBXGATE")); _ = c.Close() }, false, "CBXGATE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			go tc.server(server)
			conn, gate, err := detectWebVNCInputGate(client, 100*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			if gate != tc.gate {
				t.Fatalf("gate=%v, want %v", gate, tc.gate)
			}
			if tc.replay == "" {
				return
			}
			got := make([]byte, len(tc.replay))
			if _, err := io.ReadFull(conn, got); err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.replay {
				t.Fatalf("replayed %q, want %q", got, tc.replay)
			}
		})
	}
}

func TestInputGateFrames(t *testing.T) {
	var buf bytes.Buffer
	if err := writeInputGateData(&buf, bytes.Repeat([]byte("x"), inputGateMaxData+3)); err != nil {
		t.Fatal(err)
	}
	if err := writeInputGateFrame(&buf, inputGateControl, []byte(`{"type":"input_state"}`)); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		kind byte
		size int
	}{{inputGateData, inputGateMaxData}, {inputGateData, 3}, {inputGateControl, 22}} {
		kind, payload, err := readInputGateFrame(&buf)
		if err != nil || kind != want.kind || len(payload) != want.size {
			t.Fatalf("frame kind=%d size=%d err=%v, want %+v", kind, len(payload), err, want)
		}
	}
	if err := writeInputGateFrame(io.Discard, inputGateControl, make([]byte, inputGateMaxControl+1)); !errors.Is(err, errInputGateProtocol) {
		t.Fatalf("oversized control err=%v", err)
	}
	for _, header := range [][]byte{{7, 0, 0, 0}, {inputGateControl, 0, 0x10, 1}, {inputGateData, 0x10, 0, 1}} {
		if _, _, err := readInputGateFrame(bytes.NewReader(header)); !errors.Is(err, errInputGateProtocol) {
			t.Fatalf("header %v err=%v", header, err)
		}
	}
}

func TestInputGateMessageType(t *testing.T) {
	for data, want := range map[string]string{
		`{"type":"input_binding","binding":"x"}`: "input_binding",
		`{"type":"input_state"}`:                 "input_state",
		// The coordinator reserves neither of these, so neither is control.
		` {"type":"input_state"}`:             "",
		`{"TYPE":"input_binding"}`:            "",
		`{"type":"x","type":"input_request"}`: "input_request",
		`{"type":"desktop_theme"}`:            "",
		`{"type":"input`:                      "",
		`RFB 003.008`:                         "",
		``:                                    "",
	} {
		if got := inputGateMessageType([]byte(data)); got != want {
			t.Errorf("inputGateMessageType(%q)=%q, want %q", data, got, want)
		}
	}
}

// fakeInputGateCoordinator serves the WebVNC ticket and agent endpoints and
// hands the test the agent socket and the capabilities it advertised.
func fakeInputGateCoordinator(t *testing.T) (*httptest.Server, <-chan *websocket.Conn, <-chan string) {
	t.Helper()
	agents := make(chan *websocket.Conn, 1)
	capabilities := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/leases/cbx_abcdef123456/webvnc/ticket":
			_ = json.NewEncoder(w).Encode(CoordinatorWebVNCTicket{Ticket: "wvnc_abcdef1234567890abcdef1234567890", LeaseID: "cbx_abcdef123456"})
		case "/v1/leases/cbx_abcdef123456/webvnc/agent":
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				t.Errorf("websocket accept: %v", err)
				return
			}
			capabilities <- r.URL.Query().Get("capabilities")
			agents <- conn
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	return server, agents, capabilities
}

func startInputGateBridge(t *testing.T, ctx context.Context, greet string) (net.Conn, *websocket.Conn, string, <-chan error) {
	t.Helper()
	server, agents, capabilities := fakeInputGateCoordinator(t)
	t.Cleanup(server.Close)
	bridgeSide, gateSide := net.Pipe()
	t.Cleanup(func() { _ = gateSide.Close() })
	go func() { _, _ = gateSide.Write([]byte(greet)) }()
	coord := &CoordinatorClient{BaseURL: server.URL, Token: "test-token", Client: server.Client()}
	bridge, err := connectWebVNCBridgeWithDial(ctx, coord, "cbx_abcdef123456", "unused.invalid", "1", SSHTarget{TargetOS: targetLinux}, rfbCredentials{}, localWebVNCAuthAuto, io.Discard, func(context.Context) (net.Conn, error) {
		return bridgeSide, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bridge.Close)
	served := make(chan error, 1)
	go func() { served <- bridge.Serve(ctx) }()
	return gateSide, <-agents, <-capabilities, served
}

func TestWebVNCBridgeFramesTrafficForAnInputGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gate, agent, capabilities, served := startInputGateBridge(t, ctx, webVNCInputGateGreeting)
	if !strings.Contains(capabilities, webVNCInputGateCapability) {
		t.Fatalf("capabilities=%q, want %s", capabilities, webVNCInputGateCapability)
	}

	binding := `{"type":"input_binding","lease":"cbx_abcdef123456","binding":"b1234567890123456"}`
	for _, message := range []struct {
		typ  websocket.MessageType
		data string
	}{
		{websocket.MessageText, binding},
		{websocket.MessageBinary, "RFB 003.008\n"},
		{websocket.MessageText, `{"type":"not_input"}`},
		{websocket.MessageText, `{"type":"input_state","owner":"human"}`},
		{websocket.MessageText, `{"type":"input_request","id":"r1","action":"take"}`},
	} {
		if err := agent.Write(ctx, message.typ, []byte(message.data)); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []struct {
		kind byte
		data string
	}{
		{inputGateControl, binding},
		{inputGateData, "RFB 003.008\n"},
		// A text frame that is not an input_* message is ordinary data.
		{inputGateData, `{"type":"not_input"}`},
		// input_state only ever comes from the gate; it is not forwarded to it.
		{inputGateControl, `{"type":"input_request","id":"r1","action":"take"}`},
	} {
		kind, payload, err := readInputGateFrame(gate)
		if err != nil || kind != want.kind || string(payload) != want.data {
			t.Fatalf("gate got kind=%d %q err=%v, want %+v", kind, payload, err, want)
		}
	}

	state := `{"type":"input_state","owner":"human","holder":"self"}`
	if err := writeInputGateFrame(gate, inputGateData, []byte("RFB 003.008\n")); err != nil {
		t.Fatal(err)
	}
	if err := writeInputGateFrame(gate, inputGateControl, []byte(state)); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		typ  websocket.MessageType
		data string
	}{{websocket.MessageBinary, "RFB 003.008\n"}, {websocket.MessageText, state}} {
		typ, data, err := agent.Read(ctx)
		if err != nil || typ != want.typ || string(data) != want.data {
			t.Fatalf("coordinator got %v %q err=%v, want %+v", typ, data, err, want)
		}
	}

	// The gate may send nothing else as control.
	if err := writeInputGateFrame(gate, inputGateControl, []byte(`{"type":"input_binding"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		if !errors.Is(err, errInputGateProtocol) {
			t.Fatalf("serve err=%v, want protocol error", err)
		}
	case <-ctx.Done():
		t.Fatal("bridge kept serving after a forged control message")
	}
}

func TestWebVNCBridgeNeverSendsInputMessagesToAPlainVNCServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server, agent, capabilities, _ := startInputGateBridge(t, ctx, "RFB 003.008\n")
	if strings.Contains(capabilities, webVNCInputGateCapability) {
		t.Fatalf("capabilities=%q advertise a gate for a plain VNC server", capabilities)
	}
	got := make([]byte, 12)
	typ, data, err := agent.Read(ctx)
	if err != nil || typ != websocket.MessageBinary || string(data) != "RFB 003.008\n" {
		t.Fatalf("coordinator got %v %q err=%v", typ, data, err)
	}
	if err := agent.Write(ctx, websocket.MessageText, []byte(`{"type":"input_binding","lease":"x","binding":"y"}`)); err != nil {
		t.Fatal(err)
	}
	if err := agent.Write(ctx, websocket.MessageBinary, []byte("RFB 003.008\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(server, got); err != nil || string(got) != "RFB 003.008\n" {
		t.Fatalf("VNC server got %q err=%v", got, err)
	}
}
