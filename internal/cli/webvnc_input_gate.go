package cli

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"nhooyr.io/websocket"
)

// A guest can put an input gate in front of its VNC server so that desktop
// input is exclusive: only the viewer session that holds control reaches the
// desktop, and an agent inside the guest cannot bypass it. The gate greets a
// connection with webVNCInputGateGreeting instead of an RFB version. After
// the greeting both directions carry frames of one kind byte, a 3-byte
// big-endian length and a payload: RFB bytes (data) or one JSON object
// (control).
//
// The bridge forwards only the coordinator's input_binding and input_request
// messages as control frames, and only the gate's input_state and
// input_result messages back. The coordinator never forwards a viewer's own
// input_* messages, so a viewer cannot forge its binding. Everything a viewer
// sends travels as data, which the gate parses and filters itself.
const (
	webVNCInputGateGreeting      = "CBXGATE 001\n"
	webVNCInputGateCapability    = "input_gate"
	webVNCInputGateDetectTimeout = 3 * time.Second

	inputGateData       byte = 0
	inputGateControl    byte = 1
	inputGateMaxData         = 1 << 20
	inputGateMaxControl      = 4096
)

var errInputGateProtocol = errors.New("input gate protocol error")

// prefixedConn replays bytes read while detecting the gate.
type prefixedConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixedConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

// detectWebVNCInputGate reads the server's first bytes. It reports a gate
// only for the exact greeting; a plain VNC server's bytes are replayed.
func detectWebVNCInputGate(conn net.Conn, timeout time.Duration) (net.Conn, bool, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return conn, false, nil
	}
	buf := make([]byte, len(webVNCInputGateGreeting))
	n, err := io.ReadFull(conn, buf)
	if resetErr := conn.SetReadDeadline(time.Time{}); resetErr != nil && err == nil {
		return nil, false, resetErr
	}
	if err == nil && string(buf) == webVNCInputGateGreeting {
		return conn, true, nil
	}
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, os.ErrDeadlineExceeded) {
		return nil, false, err
	}
	return &prefixedConn{Conn: conn, prefix: buf[:n]}, false, nil
}

func withInputGateCapability(capabilities string) string {
	if strings.TrimSpace(capabilities) == "" {
		return webVNCInputGateCapability
	}
	return capabilities + "," + webVNCInputGateCapability
}

func writeInputGateFrame(w io.Writer, kind byte, payload []byte) error {
	limit := inputGateMaxData
	if kind == inputGateControl {
		limit = inputGateMaxControl
	}
	if len(payload) > limit {
		return fmt.Errorf("%w: frame too large", errInputGateProtocol)
	}
	frame := make([]byte, 4+len(payload))
	frame[0] = kind
	frame[1], frame[2], frame[3] = byte(len(payload)>>16), byte(len(payload)>>8), byte(len(payload))
	copy(frame[4:], payload)
	_, err := w.Write(frame)
	return err
}

func writeInputGateData(w io.Writer, data []byte) error {
	for len(data) > 0 {
		chunk := data
		if len(chunk) > inputGateMaxData {
			chunk = chunk[:inputGateMaxData]
		}
		if err := writeInputGateFrame(w, inputGateData, chunk); err != nil {
			return err
		}
		data = data[len(chunk):]
	}
	return nil
}

func readInputGateFrame(r io.Reader) (byte, []byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	length := int(binary.BigEndian.Uint32([]byte{0, header[1], header[2], header[3]}))
	switch header[0] {
	case inputGateData:
		if length > inputGateMaxData {
			return 0, nil, fmt.Errorf("%w: data frame too large", errInputGateProtocol)
		}
	case inputGateControl:
		if length > inputGateMaxControl {
			return 0, nil, fmt.Errorf("%w: control frame too large", errInputGateProtocol)
		}
	default:
		return 0, nil, fmt.Errorf("%w: unknown frame kind", errInputGateProtocol)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return header[0], payload, nil
}

// inputGateMessageType returns the type of a JSON object whose "type" key
// starts with "input_", or "" for anything else. It reads exactly the key the
// coordinator reads, with no leading whitespace and no case folding, so a
// frame the coordinator did not reserve is never taken for a control message.
func inputGateMessageType(data []byte) string {
	if len(data) == 0 || data[0] != '{' {
		return ""
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return ""
	}
	var kind string
	if json.Unmarshal(fields["type"], &kind) != nil || !strings.HasPrefix(kind, "input_") {
		return ""
	}
	return kind
}

// forwardInputGateText handles a text frame from the coordinator. It reports
// whether the frame was an input_* message, which never reaches a plain VNC
// server as RFB bytes.
func (b *webVNCBridge) forwardInputGateText(data []byte) (bool, error) {
	switch inputGateMessageType(data) {
	case "":
		return false, nil
	case "input_binding", "input_request":
		if !b.inputGate {
			return true, nil
		}
		return true, writeInputGateFrame(b.tcp, inputGateControl, data)
	default:
		return true, nil
	}
}

// copyInputGateToWebSocket relays the gate's frames to the coordinator: RFB
// bytes as binary messages and the gate's state and results as text.
func copyInputGateToWebSocket(ctx context.Context, ws *websocket.Conn, gate io.Reader) error {
	for {
		kind, payload, err := readInputGateFrame(gate)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if kind == inputGateData {
			if err := ws.Write(ctx, websocket.MessageBinary, payload); err != nil {
				return err
			}
			continue
		}
		switch inputGateMessageType(payload) {
		case "input_state", "input_result":
			if err := ws.Write(ctx, websocket.MessageText, payload); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unexpected control message", errInputGateProtocol)
		}
	}
}
