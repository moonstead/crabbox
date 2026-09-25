package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

// execSupervisorOptions are used by a trusted supervisor, such as the runtime
// adapter, that returns command results to another principal.
type execSupervisorOptions struct {
	statusFD           int
	expectRegistration string
	terminateRemote    bool
}

func (o execSupervisorOptions) enabled() bool {
	return o.statusFD >= 0 || o.expectRegistration != "" || o.terminateRemote
}

func (o execSupervisorOptions) validate(pty bool) error {
	if o.statusFD >= 0 && o.statusFD < 3 {
		return Exit(2, "--status-fd must name an inherited descriptor of at least 3")
	}
	if o.expectRegistration != "" && !validControllerWorkspaceID(o.expectRegistration) {
		return Exit(2, "--expect-runtime-registration must be a lowercase DNS-style registration ID")
	}
	if pty && (o.statusFD >= 0 || o.terminateRemote) {
		// A remote terminal echoes input; supervisors must not receive it as output.
		return Exit(2, "--pty cannot combine --status-fd or --terminate-remote-on-disconnect")
	}
	return nil
}

type execStatusEvent struct {
	Event  string `json:"event"`
	Reason string `json:"reason,omitempty"`
}

// execStatus separates command status from command output. Only "started"
// means the remote command may have run; any other exit is a setup failure
// whose diagnostics are not command output.
type execStatus struct {
	mu   sync.Mutex
	file *os.File
}

func openExecStatus(fd int) (*execStatus, error) {
	if fd < 0 {
		return nil, nil
	}
	file := os.NewFile(uintptr(fd), "crabbox-exec-status")
	if file == nil {
		return nil, Exit(2, "--status-fd %d is not an open descriptor", fd)
	}
	if _, err := file.Stat(); err != nil {
		_ = file.Close()
		return nil, Exit(2, "--status-fd %d is not an open descriptor", fd)
	}
	setExecStatusCloseOnExec(fd)
	return &execStatus{file: file}, nil
}

func (s *execStatus) Write(event execStatusEvent) error {
	if s == nil {
		return nil
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write exec status: %w", err)
	}
	return nil
}

func (s *execStatus) Close() {
	if s != nil {
		_ = s.file.Close()
	}
}

// runtimeAdapterRegistrationCurrent accepts only an acknowledged generation.
// A pending replacement may already be active at the coordinator, so either
// side of an unfinished rotation fails closed.
func runtimeAdapterRegistrationCurrent(claim leaseClaim, expected string) bool {
	return strings.TrimSpace(claim.RuntimeAdapterPendingRegistrationID) == "" &&
		strings.TrimSpace(claim.RuntimeAdapterRegistrationID) == expected
}

// execRemoteSessionGuard runs the command in its own process group and
// terminates that group once the SSH session process that started it exits.
// Without a PTY, sshd does not signal remote commands when the client goes
// away, so cancellation would otherwise leave the command running.
//
// Without job control, POSIX shells replace a background job's stdin with
// /dev/null before applying its redirections, so stdin is first kept on
// descriptor 9.
const execRemoteSessionGuard = `parent=$PPID
alive() {
  if [ -d /proc/self ]; then [ -d "/proc/$1" ]; else kill -0 "$1" 2>/dev/null; fi
}
exec 9<&0
if command -v setsid >/dev/null 2>&1; then
  setsid "$@" <&9 9<&- &
  target=-$!
else
  "$@" <&9 9<&- &
  target=$!
fi
child=$!
exec 9<&- </dev/null
(
  while alive "$parent" && alive "$child"; do sleep 1; done
  if alive "$child"; then
    kill -s TERM -- "$target" 2>/dev/null
    sleep 5
    kill -s KILL -- "$target" 2>/dev/null
  fi
) </dev/null >/dev/null 2>&1 &
watch=$!
wait "$child"
code=$?
kill "$watch" 2>/dev/null
exit "$code"
`

func execRemoteSessionGuardCommand(command []string) string {
	return "exec sh -c " + shellQuote(execRemoteSessionGuard) + " crabbox-exec-guard " + strings.Join(shellWords(command), " ")
}
