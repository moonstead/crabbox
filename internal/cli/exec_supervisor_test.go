//go:build !windows

package cli

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// execStatusPipe returns a descriptor that exec may close and a reader for
// the events written to it.
func execStatusPipe(t *testing.T) (int, *os.File) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	fd, err := syscall.Dup(int(writer.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	t.Cleanup(func() { _ = reader.Close() })
	return fd, reader
}

func readExecStatusEvents(t *testing.T, reader io.Reader) []string {
	t.Helper()
	var events []string
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		events = append(events, scanner.Text())
	}
	return events
}

func TestExecSupervisorStatusSeparatesPreparationFromCommandOutput(t *testing.T) {
	b, _ := setupExecCommand(t)
	b.prepareMessage = "provider preparation\n"
	fd, reader := execStatusPipe(t)
	var out, diagnostic bytes.Buffer
	err := (App{Stdin: strings.NewReader("private-input"), Stdout: &out, Stderr: &diagnostic}).Run(t.Context(), []string{
		"exec", "--id", b.lease.LeaseID, "--status-fd", strconv.Itoa(fd), "--",
		"sh", "-c", "cat; printf 'command-error' >&2; exit 7",
	})
	var commandExit ExitError
	if !AsExitError(err, &commandExit) || commandExit.Code != 7 || commandExit.Message != "" {
		t.Fatalf("exit=%#v", err)
	}
	if out.String() != "private-input" || diagnostic.String() != "command-error" {
		t.Fatalf("stdout=%q stderr=%q", out.String(), diagnostic.String())
	}
	if events := readExecStatusEvents(t, reader); len(events) != 1 || events[0] != `{"event":"started"}` {
		t.Fatalf("status events=%q", events)
	}
}

func TestExecSupervisorRegistrationFence(t *testing.T) {
	for _, test := range []struct {
		name     string
		current  string
		pending  string
		expected string
		wantRun  bool
	}{
		{name: "current generation", current: "reg-current", expected: "reg-current", wantRun: true},
		{name: "stale generation", current: "reg-next", expected: "reg-current"},
		{name: "unregistered lease", expected: "reg-current"},
		{name: "rotation pending", current: "reg-current", pending: "reg-next", expected: "reg-current"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, dir := setupExecCommand(t)
			if err := mutateLeaseClaim(b.lease.LeaseID, func(claim *leaseClaim) error {
				claim.RuntimeAdapterRegistrationID = test.current
				claim.RuntimeAdapterPendingRegistrationID = test.pending
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			fd, reader := execStatusPipe(t)
			err := (App{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard}).Run(t.Context(), []string{
				"exec", "--id", b.lease.LeaseID, "--status-fd", strconv.Itoa(fd),
				"--expect-runtime-registration", test.expected, "--", "true",
			})
			events := readExecStatusEvents(t, reader)
			_, statErr := os.Stat(filepath.Join(dir, "args"))
			if test.wantRun {
				if err != nil || len(events) != 1 || events[0] != `{"event":"started"}` || statErr != nil {
					t.Fatalf("err=%v events=%q ssh=%v", err, events, statErr)
				}
				return
			}
			var exit ExitError
			if !AsExitError(err, &exit) || exit.Code != 4 {
				t.Fatalf("stale generation error=%v", err)
			}
			if len(events) != 1 || events[0] != `{"event":"rejected","reason":"registration"}` {
				t.Fatalf("status events=%q", events)
			}
			if !errors.Is(statErr, os.ErrNotExist) || b.resolveCalls != 0 {
				t.Fatal("stale generation reached provider resolution or SSH")
			}
		})
	}
}

func TestExecSupervisorOptionValidation(t *testing.T) {
	b, dir := setupExecCommand(t)
	for _, test := range []struct {
		args []string
		want string
	}{
		{args: []string{"--status-fd", "1"}, want: "at least 3"},
		{args: []string{"--status-fd", "999"}, want: "not an open descriptor"},
		{args: []string{"--pty", "--status-fd", "3"}, want: "--pty cannot combine"},
		{args: []string{"--pty", "--terminate-remote-on-disconnect"}, want: "--pty cannot combine"},
		{args: []string{"--expect-runtime-registration", "Not_Valid"}, want: "registration ID"},
	} {
		args := append([]string{"exec", "--id", b.lease.LeaseID}, test.args...)
		args = append(args, "--", "true")
		err := (App{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard}).Run(t.Context(), args)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("args=%q error=%v", test.args, err)
		}
	}
	err := (App{Stdout: io.Discard, Stderr: io.Discard}).Run(t.Context(), []string{"exec", "--check", "--provider", b.spec.Name, "--terminate-remote-on-disconnect"})
	if err == nil || !strings.Contains(err.Error(), "supervisor options") {
		t.Fatalf("check with supervisor option=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "args")); !errors.Is(err, os.ErrNotExist) || b.resolveCalls != 0 {
		t.Fatal("rejected supervisor option reached resolution or SSH")
	}
}

func TestExecSupervisorRemoteGuardPreservesArgumentsInputAndExit(t *testing.T) {
	b, dir := setupExecCommand(t)
	var out bytes.Buffer
	err := (App{Stdin: strings.NewReader("guarded-input"), Stdout: &out, Stderr: io.Discard}).Run(t.Context(), []string{
		"exec", "--id", b.lease.LeaseID, "--terminate-remote-on-disconnect", "--",
		"sh", "-c", "cat; printf '%s' \"$1\"; exit 9", "sh", "it's $(literal)",
	})
	var exit ExitError
	if !AsExitError(err, &exit) || exit.Code != 9 {
		t.Fatalf("exit=%v", err)
	}
	if out.String() != "guarded-input"+"it's $(literal)" {
		t.Fatalf("stdout=%q", out.String())
	}
	args, err := os.ReadFile(filepath.Join(dir, "args"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "crabbox-exec-guard") {
		t.Fatalf("remote command was not guarded: %q", args)
	}
}

// The guard must terminate the whole remote process group once the process
// standing in for the SSH session exits, as sshd does not signal it.
func TestExecRemoteSessionGuardTerminatesGroupWhenSessionEnds(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "grandchild.pid")
	command := []string{"sh", "-c", `sleep 300 & printf '%s' "$!" > "$1"; wait`, "sh", pidFile}
	// "session" plays sshd: it runs the remote command string in a child shell
	// so that the guard's parent can exit independently.
	session := exec.Command("sh", "-c", `sh -c "$1"; :`, "session", execRemoteSessionGuardCommand(command))
	session.Stdin = strings.NewReader("")
	session.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := session.Start(); err != nil {
		t.Fatal(err)
	}
	var grandchild int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(pidFile); err == nil && len(data) > 0 {
			grandchild, _ = strconv.Atoi(string(data))
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if grandchild <= 0 {
		_ = session.Process.Kill()
		_ = session.Wait()
		t.Fatal("guarded command did not start")
	}
	t.Cleanup(func() { _ = syscall.Kill(grandchild, syscall.SIGKILL) })
	if err := syscall.Kill(grandchild, 0); err != nil {
		t.Fatalf("grandchild not running: %v", err)
	}
	// Kill only the session process; its guarded descendants remain.
	if err := session.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = session.Wait()
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(grandchild, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if processIsZombie(grandchild) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("guarded grandchild %d survived the end of its session", grandchild)
}

func processIsZombie(pid int) bool {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return false
	}
	fields := strings.Fields(string(data))
	return len(fields) > 2 && fields[2] == "Z"
}

// Shells without job control give background jobs /dev/null as stdin before
// applying redirections; dash does so even for "<&0". Run the guard under
// every available POSIX shell so input forwarding cannot regress.
func TestExecRemoteSessionGuardForwardsStdinInPOSIXShells(t *testing.T) {
	tested := 0
	for _, shell := range [][]string{{"sh"}, {"dash"}, {"bash"}, {"busybox", "sh"}} {
		path, err := exec.LookPath(shell[0])
		if err != nil {
			continue
		}
		tested++
		args := append(append([]string{}, shell[1:]...), "-c", execRemoteSessionGuard, "crabbox-exec-guard", "sh", "-c", "cat; exit 3")
		cmd := exec.Command(path, args...)
		cmd.Stdin = strings.NewReader("guarded-stdin")
		out, err := cmd.Output()
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 || string(out) != "guarded-stdin" {
			t.Fatalf("%s: stdout=%q err=%v", strings.Join(shell, " "), out, err)
		}
	}
	if tested == 0 {
		t.Fatal("no POSIX shell available")
	}
}
