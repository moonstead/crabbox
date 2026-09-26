package cli

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	xssh "golang.org/x/crypto/ssh"
)

func testProxmoxGuestHostKey(t *testing.T) string {
	t.Helper()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key, err := xssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(xssh.MarshalAuthorizedKey(key)))
}

func TestProxmoxGuestSSHHostKeyComesFromTheGuestAgent(t *testing.T) {
	hostKey := testProxmoxGuestHostKey(t)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	rsaPublic, err := xssh.NewPublicKey(&rsaKey.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		content   string
		truncated bool
		want      string
	}{
		{name: "ed25519 key", content: hostKey + " root@guest\n", want: hostKey},
		{name: "not generated yet", content: ""},
		{name: "not a key", content: "garbage\n"},
		{name: "rsa key", content: string(xssh.MarshalAuthorizedKey(rsaPublic))},
		{name: "truncated", content: hostKey + "\n", truncated: true},
		{name: "options are refused", content: "command=\"x\" " + hostKey + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/api2/json/nodes/pve1/qemu/101/agent/file-read" ||
					r.URL.Query().Get("file") != "/etc/ssh/ssh_host_ed25519_key.pub" {
					t.Fatalf("%s %s", r.Method, r.URL.String())
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"content": tc.content, "truncated": tc.truncated}})
			}))
			defer server.Close()
			got, err := testProxmoxClient(t, server.URL).GuestSSHHostKey(context.Background(), 101)
			if tc.want == "" {
				if err == nil {
					t.Fatalf("key=%q, want an error", got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("key=%q err=%v, want %q", got, err, tc.want)
			}
		})
	}
}

func TestPinProxmoxHostKeyChecksStrictlyByLeaseNotAddress(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	hostKey := testProxmoxGuestHostKey(t)
	target := SSHTarget{Host: "10.77.0.100", Port: "22", User: "crabbox"}
	server := Server{Labels: map[string]string{ProxmoxSSHHostKeyLabel: hostKey}}
	if err := PinProxmoxHostKey(&target, server, "cbx_123456abcdef"); err != nil {
		t.Fatal(err)
	}
	args := strings.Join(sshHostKeyVerificationArgs(target), " ")
	for _, want := range []string{"StrictHostKeyChecking=yes", "HostKeyAlias=crabbox-lease-", "GlobalKnownHostsFile=none", "CheckHostIP=no"} {
		if !strings.Contains(args, want) {
			t.Fatalf("args=%s, want %s", args, want)
		}
	}
	knownHosts, err := os.ReadFile(target.KnownHostsFile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(knownHosts), "10.77.0.100") || !strings.Contains(string(knownHosts), strings.Fields(hostKey)[1]) {
		t.Fatalf("known_hosts=%q: the key must be recorded under the lease alias, not the address", knownHosts)
	}

	// A lease with no attested key, such as an LXC lease, is left as it was
	// and reports no pinned identity.
	unpinned := SSHTarget{Host: "10.77.0.101", Port: "22", User: "crabbox"}
	if err := PinProxmoxHostKey(&unpinned, Server{Labels: map[string]string{}}, "cbx_abcdef123456"); err != nil {
		t.Fatal(err)
	}
	if unpinned.SSHHostKey != "" {
		t.Fatalf("unpinned=%#v", unpinned)
	}
}

func TestControllerReportsAPinnedSSHHostIdentity(t *testing.T) {
	record := controllerWorkspaceRecord{Status: "ready", SSHHostKeyPinned: true}
	record.Request.ID = "stead-0123456789abcdef"
	body, err := json.Marshal(controllerResponse(record))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"sshHostKeyPinned":true`) {
		t.Fatalf("response=%s", body)
	}
	record.SSHHostKeyPinned = false
	body, _ = json.Marshal(controllerResponse(record))
	if strings.Contains(string(body), "sshHostKeyPinned") {
		t.Fatalf("an unpinned lease must not claim a pinned identity: %s", body)
	}
}
