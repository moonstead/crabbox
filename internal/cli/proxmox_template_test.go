//go:build !windows

package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func proxmoxTemplateTestConfig(desktop, browser bool) Config {
	cfg := baseConfig()
	cfg.Provider, cfg.SSHUser, cfg.WorkRoot = "proxmox", "crabbox", "/work/crabbox"
	cfg.Desktop, cfg.Browser = desktop, browser
	return cfg
}

func requireBashSyntax(t *testing.T, script string) {
	t.Helper()
	cmd := exec.Command("bash", "-n")
	cmd.Stdin = strings.NewReader(script)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bash -n: %v: %s", err, output)
	}
}

func TestProxmoxHeadlessBootstrapHasNoTemplateCapabilities(t *testing.T) {
	script := proxmoxBootstrapScript(proxmoxTemplateTestConfig(false, false))
	for _, unexpected := range []string{"crabbox-xvfb", "vnc", "browser", "crabbox_require_template_packages", "crabbox_packages_installed"} {
		if strings.Contains(script, unexpected) {
			t.Fatalf("headless bootstrap contains %q", unexpected)
		}
	}
	if !strings.HasSuffix(script, "touch /var/lib/crabbox/bootstrapped\n/usr/local/bin/crabbox-ready\n") {
		t.Fatal("headless readiness no longer runs once after bootstrap")
	}
	requireBashSyntax(t, script)
}

func TestProxmoxTemplateBootstrapConfiguresWithoutInstallingPackages(t *testing.T) {
	cfg := proxmoxTemplateTestConfig(true, true)
	script := proxmoxBootstrapScript(cfg)
	requireBashSyntax(t, script)
	if strings.Contains(script, "\ncrabbox_install_packages ") || strings.Contains(script, "dl.google.com") || strings.Contains(script, "apt-get install -y --no-install-recommends chromium") {
		t.Fatal("template bootstrap installs packages")
	}
	for _, want := range []string{
		"crabbox_require_template_packages " + linuxXFCEDesktopPackages + "\n",
		"crabbox_require_template_packages " + linuxBrowserSupportPackages + "\n",
		"rm -f /var/lib/crabbox/vnc.password /var/lib/crabbox/vnc.pass\n(umask 077 && openssl rand -base64 18 > /var/lib/crabbox/vnc.password)\n",
		"rm -rf /home/crabbox/.cache/crabbox/browser-profile\n",
		linuxBrowserWrapperScript,
		"systemctl restart crabbox-xvfb.service crabbox-desktop.service\n",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("template bootstrap is missing %q", want)
		}
	}
	ready := script[strings.Index(script, "cat >/usr/local/bin/crabbox-ready <<'READY'"):strings.Index(script, "\nREADY\n")]
	for _, want := range []string{
		"systemctl is-active --quiet crabbox-xvfb.service",
		"systemctl is-active --quiet crabbox-desktop.service",
		"test -s /var/lib/crabbox/vnc.password",
		proxmoxVNCLoopbackCheck,
		`"$BROWSER" --version >/dev/null`,
	} {
		if !strings.Contains(ready, want) {
			t.Fatalf("crabbox-ready is missing %q", want)
		}
	}
	// Services and credentials are configured before the lease can be ready.
	if strings.Index(script, "systemctl restart crabbox-xvfb.service") > strings.Index(script, "touch /var/lib/crabbox/bootstrapped") {
		t.Fatal("desktop services start after the bootstrap marker")
	}
}

func TestProxmoxTemplateDesktopReusesManagedFiles(t *testing.T) {
	cfg := proxmoxTemplateTestConfig(true, false)
	files, err := proxmoxManagedDesktopFiles(cfg)
	if err != nil {
		t.Fatal(err)
	}
	script := proxmoxBootstrapScript(cfg)
	managed := cloudInit(cfg, "ssh-ed25519 AAAA test")
	want := map[string]bool{
		"/etc/systemd/system/crabbox-xvfb.service":       false,
		"/etc/systemd/system/crabbox-desktop.service":    false,
		"/usr/local/bin/crabbox-configure-desktop-theme": false,
		"/usr/local/bin/crabbox-desktop-session":         false,
	}
	for _, file := range files {
		if _, ok := want[file.Path]; ok {
			want[file.Path] = true
		}
		if !strings.Contains(script, "cat >"+shellQuote(file.Path)+" <<'CRABBOX_TEMPLATE_FILE'\n"+file.Content+"CRABBOX_TEMPLATE_FILE\nchmod "+file.Permissions+" "+shellQuote(file.Path)+"\n") {
			t.Fatalf("template bootstrap does not write %s verbatim", file.Path)
		}
		firstLine, _, _ := strings.Cut(file.Content, "\n")
		if !strings.Contains(managed, "- path: "+file.Path+"\n") || !strings.Contains(managed, firstLine) {
			t.Fatalf("%s is not a managed Linux desktop file", file.Path)
		}
	}
	for path, found := range want {
		if !found {
			t.Fatalf("managed desktop file %s is missing", path)
		}
	}
	if !strings.Contains(script, "-localhost yes -rfbport 5900") {
		t.Fatal("template desktop VNC is not loopback-only")
	}
}

func TestProxmoxVNCLoopbackCheck(t *testing.T) {
	for _, tc := range []struct {
		name      string
		listeners string
		ok        bool
	}{
		{name: "IPv4 loopback", listeners: "LISTEN 0 5 127.0.0.1:5900 0.0.0.0:*", ok: true},
		{name: "dual-stack loopback", listeners: "LISTEN 0 5 127.0.0.1:5900 0.0.0.0:*\nLISTEN 0 5 [::1]:5900 [::]:*", ok: true},
		{name: "other port only", listeners: "LISTEN 0 5 127.0.0.1:59000 0.0.0.0:*"},
		{name: "none"},
		{name: "wildcard IPv4", listeners: "LISTEN 0 5 0.0.0.0:5900 0.0.0.0:*"},
		{name: "loopback and wildcard", listeners: "LISTEN 0 5 127.0.0.1:5900 0.0.0.0:*\nLISTEN 0 5 [::]:5900 [::]:*"},
		{name: "loopback and LAN", listeners: "LISTEN 0 5 127.0.0.1:5900 0.0.0.0:*\nLISTEN 0 5 192.0.2.10:5900 0.0.0.0:*"},
		{name: "any address", listeners: "LISTEN 0 5 *:5900 *:*"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			script := "set -euo pipefail\nss() { [ \"$*\" = -Hltn ] || exit 97; printf '%s\\n' \"$LISTENERS\"; }\n" + proxmoxVNCLoopbackCheck + "\n"
			cmd := exec.CommandContext(ctx, "bash", "-c", script)
			cmd.Env = []string{"PATH=/usr/bin:/bin", "LISTENERS=" + tc.listeners}
			output, err := cmd.CombinedOutput()
			if (err == nil) != tc.ok {
				t.Fatalf("ok=%t err=%v output=%s", err == nil, err, output)
			}
		})
	}
}

func TestProxmoxCloneDescriptionRecordsTemplateCapabilities(t *testing.T) {
	var description string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api2/json/nodes/pve1/qemu/9000/clone" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		description = readForm(t, r).Get("description")
		http.Error(w, "stop after clone request", http.StatusInternalServerError)
	}))
	defer server.Close()
	client := testProxmoxClient(t, server.URL)
	cfg := proxmoxTemplateTestConfig(true, true)
	cfg.Proxmox.Node, cfg.Proxmox.TemplateID = "pve1", 9000
	if _, err := client.CreateServerWithVMID(context.Background(), cfg, "ssh-ed25519 AAAA test", "cbx_123456abcdef", "desktop", false, 417, map[string]string{"fixed_intent_sha256": "fixture"}, func(Server) error { return nil }); err == nil {
		t.Fatal("expected fixture clone failure")
	}
	labels := proxmoxDescriptionLabels(description)
	if labels["desktop"] != "true" || labels["desktop_env"] != "xfce" || labels["browser"] != "true" {
		t.Fatalf("clone description labels=%v", labels)
	}
}
