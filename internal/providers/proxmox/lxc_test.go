package proxmox

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/openclaw/crabbox/internal/cli"
)

const (
	lxcTestTemplate   = "local:vztmpl/stead-headless-lxc-2404-20260926-0123456789ab.tar.zst"
	lxcTestGeneration = "0123456789abcdef0123456789abcdef"
)

func lxcControllerTestConfig() core.Config {
	cfg := controllerTestConfig()
	cfg.Proxmox.TemplateID = 0
	cfg.Proxmox.FullClone = true
	cfg.Proxmox.Guest = core.ProxmoxGuestLXC
	cfg.Proxmox.LXCTemplate = lxcTestTemplate
	cfg.Proxmox.LXCCores = 2
	cfg.Proxmox.LXCMemoryMiB = 4096
	cfg.Proxmox.LXCSwapMiB = 512
	cfg.Proxmox.LXCDiskGiB = 16
	return cfg
}

// Scopes and fingerprints recorded by earlier releases must not change: an
// adapter refuses workspaces whose persisted scope no longer matches.
func TestProxmoxQEMUScopeAndFingerprintAreUnchangedByLXCSupport(t *testing.T) {
	if got, want := controllerTestScope(t, controllerTestConfig()), "proxmox-v1:sha256:2996dded8dd6bf4f9466be44a2a450ce235c3e31228df0c971c5ed6cdba8c388"; got != want {
		t.Fatalf("QEMU controller scope changed: %s", got)
	}
	cfg := controllerTestConfig()
	cfg.TTL, cfg.IdleTimeout = 2*time.Hour, 2*time.Hour
	cfg.ServerType = (Provider{}).ServerTypeForConfig(cfg)
	req := core.AcquireRequest{RequestedLeaseID: "cbx_0123456789ab", RequestedSlug: "blue-crab", Keep: true}
	fingerprint, err := fixedProxmoxFingerprint(cfg, req, "endpoint:https://pve.example.test:8006|node:pve1", "ssh-ed25519 AAAAfixture")
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint != "f8cd3d40ce0db8e98edd520867ef13105d5542c24c497c01d22930f12665d628" {
		t.Fatalf("QEMU fixed fingerprint changed: %s", fingerprint)
	}
	if got := (Provider{}).ServerTypeForConfig(lxcControllerTestConfig()); got != "lxc" {
		t.Fatalf("LXC server type=%q", got)
	}
}

func TestProxmoxLXCControllerScopeBindsContainerProfile(t *testing.T) {
	base := controllerTestScope(t, lxcControllerTestConfig())
	seen := map[string]string{base: "base", controllerTestScope(t, controllerTestConfig()): "qemu"}
	if len(seen) != 2 {
		t.Fatal("LXC and QEMU configurations share a scope")
	}
	for name, mutate := range map[string]func(*core.Config){
		"template": func(cfg *core.Config) { cfg.Proxmox.LXCTemplate = "local:vztmpl/stead-headless-lxc-next.tar.zst" },
		"cores":    func(cfg *core.Config) { cfg.Proxmox.LXCCores = 4 },
		"memory":   func(cfg *core.Config) { cfg.Proxmox.LXCMemoryMiB = 8192 },
		"swap":     func(cfg *core.Config) { cfg.Proxmox.LXCSwapMiB = 0 },
		"disk":     func(cfg *core.Config) { cfg.Proxmox.LXCDiskGiB = 32 },
		"pool":     func(cfg *core.Config) { cfg.Proxmox.Pool = "stead" },
		"user":     func(cfg *core.Config) { cfg.Proxmox.User = "runner" },
	} {
		cfg := lxcControllerTestConfig()
		mutate(&cfg)
		scope := controllerTestScope(t, cfg)
		if previous, ok := seen[scope]; ok {
			t.Fatalf("%s change produced the same scope as %s", name, previous)
		}
		seen[scope] = name
	}
	cfg := lxcControllerTestConfig()
	cfg.Proxmox.FullClone = false
	if controllerTestScope(t, cfg) != base {
		t.Fatal("clone mode changed an LXC scope")
	}
	for _, private := range []string{controllerTestSecret, "runner@pve", "stead-headless"} {
		if strings.Contains(base, private) {
			t.Fatalf("scope %q exposes %q", base, private)
		}
	}
	for name, mutate := range map[string]func(*core.Config){
		"missing template": func(cfg *core.Config) { cfg.Proxmox.LXCTemplate = "" },
		"qemu template":    func(cfg *core.Config) { cfg.Proxmox.TemplateID = 9420 },
		"desktop template": func(cfg *core.Config) { cfg.Proxmox.TemplateDesktop = true },
		"root user":        func(cfg *core.Config) { cfg.Proxmox.User = "root" },
		"unbounded cores":  func(cfg *core.Config) { cfg.Proxmox.LXCCores = 0 },
		"unknown guest":    func(cfg *core.Config) { cfg.Proxmox.Guest = "kvm" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := lxcControllerTestConfig()
			mutate(&cfg)
			if scope, err := (Provider{}).ControllerProviderScope(cfg); err == nil || scope != "" {
				t.Fatalf("scope=%q err=%v", scope, err)
			}
		})
	}
}

func lxcFixedFixture(t *testing.T) (*leaseBackend, *fixedProxmoxClient, core.AcquireRequest) {
	t.Helper()
	backend, client, req := fixedProxmoxFixture(t)
	cfg := backend.Cfg
	cfg.Proxmox.TemplateID = 0
	cfg.Proxmox.Guest = core.ProxmoxGuestLXC
	cfg.Proxmox.LXCTemplate = lxcTestTemplate
	cfg.Proxmox.LXCCores, cfg.Proxmox.LXCMemoryMiB, cfg.Proxmox.LXCSwapMiB, cfg.Proxmox.LXCDiskGiB = 2, 4096, 512, 16
	backend = NewLeaseBackend(Provider{}.Spec(), cfg, backend.RT).(*leaseBackend)
	return backend, client, req
}

func TestProxmoxLXCFixedLeaseCreatesReplaysAndReleasesExactContainer(t *testing.T) {
	backend, client, req := lxcFixedFixture(t)
	var createdLabels map[string]string
	client.beforeClone = func(vmid int, labels map[string]string) error {
		claim := readFixedProxmoxClaim(t, req.RequestedLeaseID)
		if claim.FixedCreateIntent == nil || claim.FixedCreateIntent.State != "prepared" || claim.CloudID != "417" || vmid != 417 {
			t.Fatalf("create preceded durable intent: %+v", claim)
		}
		createdLabels = labels
		return nil
	}
	first, err := backend.Acquire(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if createdLabels["template_id"] != lxcTestTemplate {
		t.Fatalf("identity labels=%v", createdLabels)
	}
	// This fixture exercises the fixed-claim transaction, not Proxmox HTTP:
	// the container endpoints are covered by internal/cli's HTTP tests.
	if first.Server.ImmutableID != lxcTestGeneration || first.Server.Labels[core.ProxmoxLXCGenerationLabel] != lxcTestGeneration {
		t.Fatalf("fixed LXC lease is not bound to its generation label: %+v", first.Server)
	}
	replayed, err := backend.Acquire(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if client.fixedCreates != 1 || client.nextCalls != 1 || replayed.Server.CloudID != first.Server.CloudID || replayed.Server.ImmutableID != first.Server.ImmutableID {
		t.Fatalf("replay created again: creates=%d first=%+v replay=%+v", client.fixedCreates, first.Server, replayed.Server)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := backend.ReleaseLease(context.Background(), core.ReleaseLeaseRequest{Lease: first}); err != nil {
			t.Fatalf("release %d: %v", attempt+1, err)
		}
	}
	claim := readFixedProxmoxClaim(t, req.RequestedLeaseID)
	if claim.FixedCreateIntent.State != "released" || client.deleteCalls != 1 || len(client.servers) != 0 {
		t.Fatalf("release state=%s deletes=%d servers=%d", claim.FixedCreateIntent.State, client.deleteCalls, len(client.servers))
	}
}

func TestProxmoxLXCRefusesDesktopBrowserAndCodeLeases(t *testing.T) {
	for _, capability := range []string{"desktop", "browser", "code"} {
		t.Run(capability, func(t *testing.T) {
			backend, client, req := lxcFixedFixture(t)
			switch capability {
			case "desktop":
				backend.Cfg.Desktop = true
			case "browser":
				backend.Cfg.Browser = true
			case "code":
				backend.Cfg.Code = true
			}
			if _, err := backend.Acquire(context.Background(), req); err == nil || !strings.Contains(err.Error(), "headless") {
				t.Fatalf("err=%v", err)
			}
			if client.fixedCreates != 0 || client.nextCalls != 0 {
				t.Fatalf("refused lease reached Proxmox: creates=%d next=%d", client.fixedCreates, client.nextCalls)
			}
		})
	}
}
