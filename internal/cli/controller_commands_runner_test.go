//go:build !windows

package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func commandRunnerFixture(t *testing.T, body string) (*execControllerWorkspaceRunner, string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "crabbox")
	script := "#!/bin/sh\nset -eu\nout=" + shellQuote(dir) + "\nprintf '%s\\n' \"$@\" >\"$out/args\"\nenv >\"$out/env\"\n" + body
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	runner := &execControllerWorkspaceRunner{opts: execControllerRunnerOptions{Binary: binary, Provider: "fixture", StateFile: filepath.Join(dir, "state.json"), WorkDir: dir}}
	return runner, dir
}

func TestExecControllerRunnerDeliversCommandStdinPrivately(t *testing.T) {
	runner, dir := commandRunnerFixture(t, "cat >\"$out/stdin\"\nprintf 'enrolled\\n'\nprintf 'warning\\n' >&2\nexit 7\n")
	var stdout, stderr bytes.Buffer
	code, err := runner.RunWorkspaceCommand(context.Background(), "cbx_abcdef123456", controllerWorkspaceRequest{ID: "command-box"}, commandTestArgv, []byte(commandTestSecret), &stdout, &stderr)
	if err != nil || code != 7 || stdout.String() != "enrolled\n" || stderr.String() != "warning\n" {
		t.Fatalf("code=%d err=%v stdout=%q stderr=%q", code, err, stdout.String(), stderr.String())
	}
	read := func(name string) string {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	if got := read("stdin"); got != commandTestSecret {
		t.Fatalf("stdin=%q", got)
	}
	wantArgs := strings.Join([]string{"exec", "--id", "cbx_abcdef123456", "--provider", "fixture", "--", commandTestArgv[0], commandTestArgv[1]}, "\n") + "\n"
	if got := read("args"); got != wantArgs {
		t.Fatalf("args=%q want %q", got, wantArgs)
	}
	if strings.Contains(read("env"), commandTestSecret) {
		t.Fatal("stdin reached the child environment")
	}
}

func TestExecControllerRunnerCommandWithoutInputGetsEOF(t *testing.T) {
	runner, dir := commandRunnerFixture(t, "cat >\"$out/stdin\"\n")
	code, err := runner.RunWorkspaceCommand(context.Background(), "cbx_abcdef123456", controllerWorkspaceRequest{ID: "command-box"}, commandTestArgv, nil, &bytes.Buffer{}, &bytes.Buffer{})
	if err != nil || code != 0 {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if data, err := os.ReadFile(filepath.Join(dir, "stdin")); err != nil || len(data) != 0 {
		t.Fatalf("stdin=%q err=%v", data, err)
	}
}

func TestExecControllerRunnerCommandCancellationStopsProcessTree(t *testing.T) {
	runner, dir := commandRunnerFixture(t, "sleep 30 &\necho $! >\"$out/grandchild\"\nwait\n")
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := runner.RunWorkspaceCommand(ctx, "cbx_abcdef123456", controllerWorkspaceRequest{ID: "command-box"}, commandTestArgv, []byte("input"), &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || time.Since(started) > 10*time.Second {
		t.Fatalf("err=%v elapsed=%s", err, time.Since(started))
	}
	data, readErr := os.ReadFile(filepath.Join(dir, "grandchild"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	deadline := time.Now().Add(5 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("command grandchild %d survived cancellation", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestExecControllerRunnerRejectsUntrackedCommandChild(t *testing.T) {
	runner := &execControllerWorkspaceRunner{opts: execControllerRunnerOptions{Binary: "/bin/false", Provider: "fixture"}}
	if _, err := runner.RunWorkspaceCommand(context.Background(), "cbx_abcdef123456", controllerWorkspaceRequest{ID: "command-box"}, commandTestArgv, nil, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "durable child registry") {
		t.Fatalf("untracked command child err=%v", err)
	}
	if _, err := runner.RunWorkspaceCommand(context.Background(), "cbx_ABC", controllerWorkspaceRequest{ID: "command-box"}, commandTestArgv, nil, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("noncanonical lease ID accepted")
	}
}

func TestExecControllerRunnerCommandIgnoringLargeInputReturns(t *testing.T) {
	runner, _ := commandRunnerFixture(t, "exit 3\n")
	input := bytes.Repeat([]byte("i"), 256<<10)
	code, err := runner.RunWorkspaceCommand(context.Background(), "cbx_abcdef123456", controllerWorkspaceRequest{ID: "command-box"}, commandTestArgv, input, &bytes.Buffer{}, &bytes.Buffer{})
	clear(input)
	if err != nil || code != 3 {
		t.Fatalf("code=%d err=%v", code, err)
	}
}

func TestExecControllerRunnerChecksExecutionSupportOffline(t *testing.T) {
	for _, tc := range []struct {
		output string
		want   bool
	}{
		{`{"provider":"fixture","target":"linux","execution":true,"currentRepoStop":true}`, true},
		{`{"provider":"fixture","target":"linux","execution":false,"currentRepoStop":false}`, false},
	} {
		runner, dir := commandRunnerFixture(t, "printf '%s\\n' "+shellQuote(tc.output)+"\n")
		got, err := runner.CommandExecutionSupported(context.Background())
		if err != nil || got != tc.want {
			t.Fatalf("supported=%t err=%v", got, err)
		}
		args, err := os.ReadFile(filepath.Join(dir, "args"))
		if err != nil || string(args) != "exec\n--check\n--provider\nfixture\n" {
			t.Fatalf("args=%q err=%v", args, err)
		}
	}
}
