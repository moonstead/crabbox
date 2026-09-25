package proxmox

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/openclaw/crabbox/internal/cli"
)

var _ core.ExecLeaseClaimResolver = (*leaseBackend)(nil)

// stubExecSSHReady replaces the SSH readiness probe; ready reports whether the
// guest answers, on port 22 as the template's sshd does.
func stubExecSSHReady(t *testing.T, ready bool) *int {
	t.Helper()
	calls := 0
	previous := probeExecSSHReady
	probeExecSSHReady = func(_ context.Context, target *core.SSHTarget, _ time.Duration) bool {
		calls++
		if ready {
			target.Port = "22"
		}
		return ready
	}
	t.Cleanup(func() { probeExecSSHReady = previous })
	return &calls
}

func TestProxmoxAdvertisesClaimFencedExecution(t *testing.T) {
	if !(Provider{}).Spec().Features.Has(core.FeatureClaimExec) {
		t.Fatal("Proxmox does not advertise claim-exec")
	}
}

func TestProxmoxExecResolvesOnlyTheExactReadyFixedVM(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*leaseBackend, *fixedProxmoxClient, *core.LeaseClaim)
		want   string
	}{
		{name: "completed fixed lease"},
		{
			name: "ordinary lease claim",
			change: func(_ *leaseBackend, _ *fixedProxmoxClient, claim *core.LeaseClaim) {
				claim.FixedCreateIntent = nil
			},
			want: "completed fixed-ID Proxmox lease",
		},
		{
			name: "acquisition still prepared",
			change: func(_ *leaseBackend, _ *fixedProxmoxClient, claim *core.LeaseClaim) {
				intent := *claim.FixedCreateIntent
				intent.State = "prepared"
				claim.FixedCreateIntent = &intent
			},
			want: "completed fixed-ID Proxmox lease",
		},
		{
			name: "configured cluster scope changed",
			change: func(backend *leaseBackend, _ *fixedProxmoxClient, _ *core.LeaseClaim) {
				backend.Cfg.Proxmox.Node = "pve2"
			},
			want: "provider scope changed",
		},
		{
			name: "VM regenerated under the same VMID",
			change: func(_ *leaseBackend, client *fixedProxmoxClient, _ *core.LeaseClaim) {
				client.servers[0].ImmutableID = "11111111-2222-3333-4444-555555555555"
			},
			want: "vmgenid",
		},
		{
			name: "VM relabelled for another lease",
			change: func(_ *leaseBackend, client *fixedProxmoxClient, _ *core.LeaseClaim) {
				client.servers[0].Labels["lease"] = "cbx_000000000000"
			},
			want: "does not match",
		},
		{
			name: "VM gone",
			change: func(_ *leaseBackend, client *fixedProxmoxClient, _ *core.LeaseClaim) {
				client.servers = nil
			},
			want: "no live VM",
		},
		{
			name: "VM not ready",
			change: func(_ *leaseBackend, client *fixedProxmoxClient, _ *core.LeaseClaim) {
				client.servers[0].Labels["state"] = "provisioning"
			},
			want: "not ready",
		},
		{
			name: "lease expired",
			change: func(_ *leaseBackend, client *fixedProxmoxClient, _ *core.LeaseClaim) {
				client.servers[0].Labels["expires_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
			},
			want: "no longer usable",
		},
		{
			name: "no guest address",
			change: func(_ *leaseBackend, client *fixedProxmoxClient, _ *core.LeaseClaim) {
				client.servers[0].PublicNet.IPv4.IP = ""
			},
			want: "no guest address",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, client, req := fixedProxmoxFixture(t)
			probes := stubExecSSHReady(t, true)
			acquired, err := backend.Acquire(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			claim := readFixedProxmoxClaim(t, req.RequestedLeaseID)
			if test.change != nil {
				test.change(backend, client, &claim)
			}
			lease, err := backend.ResolveExecLeaseUnderClaim(context.Background(), core.ResolveRequest{ID: req.RequestedLeaseID, Repo: req.Repo, Prepare: true}, claim)
			if test.want != "" {
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("error=%v, want %q", err, test.want)
				}
				if *probes != 0 {
					t.Fatal("rejected lease reached the guest over SSH")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if lease.LeaseID != req.RequestedLeaseID || lease.Server.CloudID != acquired.Server.CloudID ||
				lease.Server.ImmutableID != fixedTestGeneration || lease.SSH.Host != "192.0.2.17" || lease.SSH.User != "crabbox" ||
				lease.SSH.Port != "22" || *probes != 1 {
				t.Fatalf("lease=%+v", lease)
			}
		})
	}
}

func TestProxmoxExecRejectsAnotherRepositoryBeforeProviderAccess(t *testing.T) {
	backend, client, req := fixedProxmoxFixture(t)
	if _, err := backend.Acquire(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	claim := readFixedProxmoxClaim(t, req.RequestedLeaseID)
	previousClient := newClient
	calls := 0
	newClient = func(cfg core.Config) (proxmoxClient, error) { calls++; return client, nil }
	t.Cleanup(func() { newClient = previousClient })
	_, err := backend.ResolveExecLeaseUnderClaim(context.Background(), core.ResolveRequest{ID: req.RequestedLeaseID, Repo: core.Repo{Root: t.TempDir()}, Prepare: true}, claim)
	if err == nil || !strings.Contains(err.Error(), "claimed by") || calls != 0 {
		t.Fatalf("error=%v provider clients=%d", err, calls)
	}
}

func TestProxmoxExecRequiresReachableGuestSSH(t *testing.T) {
	backend, _, req := fixedProxmoxFixture(t)
	if _, err := backend.Acquire(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	claim := readFixedProxmoxClaim(t, req.RequestedLeaseID)
	stubExecSSHReady(t, false)
	_, err := backend.ResolveExecLeaseUnderClaim(context.Background(), core.ResolveRequest{ID: req.RequestedLeaseID, Repo: req.Repo, Prepare: true}, claim)
	var exit core.ExitError
	if !core.AsExitError(err, &exit) || exit.Code != 5 || !strings.Contains(err.Error(), "not reachable over SSH") {
		t.Fatalf("unreachable guest error=%v", err)
	}
}
