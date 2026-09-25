package proxmox

import (
	"context"
	"strings"
	"time"

	core "github.com/openclaw/crabbox/internal/cli"
)

const proxmoxExecSSHReadyTimeout = 30 * time.Second

// probeExecSSHReady selects the guest's working SSH port, as acquisition does,
// and confirms readiness before a command is started.
var probeExecSSHReady = core.ProbeSSHReady

// ResolveExecLeaseUnderClaim admits only completed fixed-ID leases. Their
// release takes the exclusive claim fence that exec holds shared, so a VM
// cannot be deleted or rebound while a command is running.
func (b *leaseBackend) ResolveExecLeaseUnderClaim(ctx context.Context, req core.ResolveRequest, original core.LeaseClaim) (core.LeaseTarget, error) {
	if !fixedProxmoxLeaseKind.IsFixedClaim(original) || original.FixedCreateIntent == nil || original.FixedCreateIntent.State != "acquired" {
		return core.LeaseTarget{}, core.Exit(4, "exec requires a completed fixed-ID Proxmox lease; use run for ordinary leases")
	}
	scope := strings.TrimSpace(core.ProviderClaimScope("proxmox", b.Cfg))
	if scope == "" || original.ProviderScope != scope || original.FixedCreateIntent.ProviderScope != scope {
		return core.LeaseTarget{}, core.Exit(4, "lease_id_conflict: fixed Proxmox lease %s provider scope changed before exec", original.LeaseID)
	}
	if original.CloudImmutableID == "" {
		return core.LeaseTarget{}, core.Exit(4, "lease_id_conflict: fixed Proxmox lease %s has no bound vmgenid", original.LeaseID)
	}
	vmid, node, err := fixedProxmoxAttempt(original)
	if err != nil {
		return core.LeaseTarget{}, err
	}
	if err := validateFixedProxmoxLocalBinding(original); err != nil {
		return core.LeaseTarget{}, err
	}
	if err := core.CheckLeaseClaimRepositoryOwner(original.LeaseID, original, req.Repo.Root, false); err != nil {
		return core.LeaseTarget{}, err
	}
	client, err := newClient(b.Cfg)
	if err != nil {
		return core.LeaseTarget{}, err
	}
	server, found, err := b.findFixedProxmoxServer(ctx, client, original)
	if err != nil {
		return core.LeaseTarget{}, err
	}
	if !found {
		return core.LeaseTarget{}, core.Exit(4, "fixed Proxmox lease %s has no live VM", original.LeaseID)
	}
	if err := validateFixedProxmoxServer(server, original, vmid, node); err != nil {
		return core.LeaseTarget{}, err
	}
	if server.Labels["state"] != "ready" {
		return core.LeaseTarget{}, core.Exit(4, "fixed Proxmox lease %s is not ready", original.LeaseID)
	}
	if expired, reason := core.ShouldCleanupServer(server, time.Now().UTC()); expired {
		return core.LeaseTarget{}, core.Exit(4, "fixed Proxmox lease %s is no longer usable: %s", original.LeaseID, reason)
	}
	if strings.TrimSpace(server.PublicNet.IPv4.IP) == "" {
		return core.LeaseTarget{}, core.Exit(5, "fixed Proxmox lease %s has no guest address", original.LeaseID)
	}
	target, err := b.targetForServer(server, false)
	if err != nil {
		return core.LeaseTarget{}, err
	}
	if !probeExecSSHReady(ctx, &target.SSH, proxmoxExecSSHReadyTimeout) {
		if ctx.Err() != nil {
			return core.LeaseTarget{}, context.Cause(ctx)
		}
		return core.LeaseTarget{}, core.Exit(5, "fixed Proxmox lease %s is not reachable over SSH", original.LeaseID)
	}
	target.LeaseID = original.LeaseID
	return target, nil
}
