package proxmox

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	core "github.com/openclaw/crabbox/internal/cli"
	"github.com/openclaw/crabbox/internal/testutil"
)

func TestProxmoxTemplateCapabilityAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*core.Config)
		want   string
	}{
		{"desktop without template", func(cfg *core.Config) { cfg.Desktop = true }, "proxmox.templateDesktop"},
		{"browser without template", func(cfg *core.Config) { cfg.Browser = true }, "proxmox.templateBrowser"},
		{"browser template is not a desktop", func(cfg *core.Config) { cfg.Desktop, cfg.Proxmox.TemplateBrowser = true, true }, "proxmox.templateDesktop"},
		{"wayland desktop", func(cfg *core.Config) {
			cfg.Desktop, cfg.DesktopEnv, cfg.Proxmox.TemplateDesktop = true, "wayland", true
		}, "desktopEnv=xfce only"},
		{"other guest user", func(cfg *core.Config) {
			cfg.Desktop, cfg.Proxmox.TemplateDesktop, cfg.SSHUser = true, true, "runner"
		}, "proxmox.user=crabbox"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, client, req := fixedProxmoxFixture(t)
			tc.mutate(&backend.Cfg)
			_, err := backend.Acquire(context.Background(), req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want %q", err, tc.want)
			}
			if client.nextCalls != 0 || client.fixedCreates != 0 {
				t.Fatalf("rejected capability reached Proxmox: next=%d clones=%d", client.nextCalls, client.fixedCreates)
			}
			if _, exists, err := core.ReadLeaseClaimWithPresence(req.RequestedLeaseID); err != nil || exists {
				t.Fatalf("rejected capability wrote a claim: exists=%t err=%v", exists, err)
			}
			req.RequestedLeaseID = ""
			if _, err := backend.Acquire(context.Background(), req); err == nil || !strings.Contains(err.Error(), tc.want) || len(client.leaseIDs) != 0 {
				t.Fatalf("ordinary acquisition err=%v creates=%d", err, len(client.leaseIDs))
			}
		})
	}
}

func TestProxmoxFixedTemplateDesktopBindsRequestedCapabilities(t *testing.T) {
	backend, client, req := fixedProxmoxFixture(t)
	backend.Cfg.Desktop, backend.Cfg.Browser = true, true
	backend.Cfg.Proxmox.TemplateDesktop, backend.Cfg.Proxmox.TemplateBrowser = true, true
	if _, err := backend.Acquire(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	claim := readFixedProxmoxClaim(t, req.RequestedLeaseID)
	fingerprint := func(desktop, browser bool) string {
		cfg := backend.Cfg
		cfg.Desktop, cfg.Browser = desktop, browser
		cfg.ServerType = (Provider{}).ServerTypeForConfig(cfg)
		cfg.ProviderKey = core.ProviderKeyForLease(req.RequestedLeaseID)
		value, err := fixedProxmoxFingerprint(cfg, req, claim.ProviderScope, readFixedProxmoxPublicKey(t, req.RequestedLeaseID))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if claim.FixedCreateIntent.Fingerprint != fingerprint(true, true) {
		t.Fatal("test fingerprint does not reproduce the persisted intent")
	}
	if fingerprint(false, false) == fingerprint(true, true) || fingerprint(true, false) == fingerprint(true, true) {
		t.Fatal("fixed intent does not bind the requested capabilities")
	}
	if _, err := backend.Acquire(context.Background(), req); err != nil {
		t.Fatalf("identical desktop replay: %v", err)
	}
	for name, mutate := range map[string]func(*core.Config){
		"headless":   func(cfg *core.Config) { cfg.Desktop, cfg.Browser = false, false },
		"no browser": func(cfg *core.Config) { cfg.Browser = false },
	} {
		cfg := backend.Cfg
		mutate(&backend.Cfg)
		if _, err := backend.Acquire(context.Background(), req); err == nil || !strings.Contains(err.Error(), "another create intent") {
			t.Fatalf("%s replay reused a desktop VM: %v", name, err)
		}
		backend.Cfg = cfg
	}
	if client.fixedCreates != 1 {
		t.Fatalf("capability replay cloned %d VMs", client.fixedCreates)
	}
}

// Capability flags must reach the provider through the normal warmup command.
func TestProxmoxWarmupDesktopRequiresDeclaredTemplate(t *testing.T) {
	_, client, _ := fixedProxmoxFixture(t)
	testutil.IsolateUserDirs(t)
	cfg := controllerFreeTestConfig()
	for key, value := range map[string]string{
		"CRABBOX_PROVIDER":             "proxmox",
		"CRABBOX_PROXMOX_API_URL":      cfg.Proxmox.APIURL,
		"CRABBOX_PROXMOX_TOKEN_ID":     cfg.Proxmox.TokenID,
		"CRABBOX_PROXMOX_TOKEN_SECRET": cfg.Proxmox.TokenSecret,
		"CRABBOX_PROXMOX_NODE":         cfg.Proxmox.Node,
		"CRABBOX_PROXMOX_TEMPLATE_ID":  strconv.Itoa(cfg.Proxmox.TemplateID),
	} {
		t.Setenv(key, value)
	}
	run := func(args ...string) error {
		var stdout, stderr bytes.Buffer
		return (core.App{Stdout: &stdout, Stderr: &stderr}).Run(t.Context(), args)
	}
	if err := run("warmup", "--keep=true", "--desktop", "--lease-id", "cbx_0123456789ab", "--slug", "desktop-box"); err == nil || !strings.Contains(err.Error(), "proxmox.templateDesktop") {
		t.Fatalf("undeclared desktop template: %v", err)
	}
	if client.fixedCreates != 0 {
		t.Fatal("undeclared desktop template reached Proxmox")
	}
	t.Setenv("CRABBOX_PROXMOX_TEMPLATE_DESKTOP", "true")
	if err := run("warmup", "--keep=true", "--desktop", "--lease-id", "cbx_0123456789ab", "--slug", "desktop-box"); err != nil {
		t.Fatalf("declared desktop template: %v", err)
	}
	if client.fixedCreates != 1 {
		t.Fatalf("declared desktop template clones=%d", client.fixedCreates)
	}
}

func controllerFreeTestConfig() core.Config {
	cfg := core.BaseConfig()
	cfg.Proxmox = core.ProxmoxConfig{
		APIURL: "https://pve.example.test:8006", TokenID: "runner@pve!crabbox", TokenSecret: "secret",
		Node: "pve1", TemplateID: 9400, User: "crabbox", WorkRoot: "/work/crabbox", FullClone: true,
	}
	return cfg
}

func readFixedProxmoxPublicKey(t *testing.T, leaseID string) string {
	t.Helper()
	path, err := core.TestboxKeyPath(leaseID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path + ".pub")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
