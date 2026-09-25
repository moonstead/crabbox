package proxmox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	core "github.com/openclaw/crabbox/internal/cli"
	"github.com/openclaw/crabbox/internal/providers/shared"
)

// A clone request accepted before recovery took the claim lock creates its
// VMID config as soon as its worker starts. The grace outlasts the 30-second
// Proxmox API proxy timeout and queued API workers, so absence held across it
// means the request either created nothing or its VM has since gone.
var (
	fixedRecoveryAbsenceGrace = 5 * time.Minute
	fixedRecoveryPollInterval = 15 * time.Second
)

const fixedRecoveryEvidenceKind = "proxmox-fixed-attempt-recovery"

// fixedRecoveryEvidence is written before settlement and for every retained
// proof. It binds the exact claim bytes, scope and attempt to each check.
type fixedRecoveryEvidence struct {
	Version       int                  `json:"version"`
	Kind          string               `json:"kind"`
	Outcome       string               `json:"outcome"`
	Reason        string               `json:"reason,omitempty"`
	LeaseID       string               `json:"leaseId"`
	ClaimProvider string               `json:"claimProvider"`
	ProviderScope string               `json:"providerScope"`
	Fingerprint   string               `json:"fingerprint"`
	ClaimRevision string               `json:"claimRevision"`
	ClaimSHA256   string               `json:"claimSha256"`
	JournalPhase  string               `json:"journalPhase,omitempty"`
	Attempt       string               `json:"attempt"`
	VMID          int                  `json:"vmid,omitempty"`
	Node          string               `json:"node,omitempty"`
	Grace         string               `json:"grace"`
	StartedAt     string               `json:"startedAt"`
	FinishedAt    string               `json:"finishedAt"`
	Checks        []fixedRecoveryCheck `json:"checks"`
}

type fixedRecoveryCheck struct {
	At     string `json:"at"`
	Absent bool   `json:"absent"`
	Error  string `json:"error,omitempty"`
}

// ReclaimAndStop never adopts a Proxmox VM. For `stop --force` it settles only
// a prepared fixed claim with no bound vmgenid, whose clone outcome is unknown,
// after repeated authoritative cluster absence across fixedRecoveryAbsenceGrace.
func (b *leaseBackend) ReclaimAndStop(ctx context.Context, req core.StopRequest) error {
	leaseID := req.ID
	if !core.IsCanonicalLeaseID(leaseID) {
		return core.Exit(2, "provider=proxmox stop --force requires an exact canonical lease ID")
	}
	claim, exists, err := core.ReadLeaseClaimWithPresence(leaseID)
	if err != nil {
		return err
	}
	if !exists || !fixedProxmoxLeaseKind.IsFixedClaim(claim) {
		return core.Exit(2, "provider=proxmox stop --force only recovers an unresolved fixed-ID clone attempt; lease %s has no fixed Proxmox claim", leaseID)
	}
	scope := strings.TrimSpace(core.ProviderClaimScope("proxmox", b.Cfg))
	if scope == "" || claim.ProviderScope != scope || claim.FixedCreateIntent.ProviderScope != scope {
		return core.Exit(4, "lease_id_conflict: fixed Proxmox lease %s belongs to another cluster scope", leaseID)
	}
	if claim.FixedCreateIntent.State == "released" {
		if err := fixedProxmoxLeaseKind.ValidateTerminalClaim(claim, core.LeaseClaim{}, leaseID, validateFixedProxmoxTerminalClaim); err != nil {
			return err
		}
		removeUnboundFixedProxmoxKey(claim)
		fmt.Fprintf(b.RT.Stderr, "lease=%s fixed Proxmox attempt is already settled\n", leaseID)
		return nil
	}
	if claim.FixedCreateIntent.State != "prepared" || claim.CloudImmutableID != "" {
		return core.Exit(4, "lease_id_conflict: fixed Proxmox lease %s has a bound or deleting clone; use crabbox stop without --force", leaseID)
	}
	vmid, node, err := fixedProxmoxAttempt(claim)
	if err != nil {
		return err
	}
	if vmid != 0 {
		if node != strings.TrimSpace(b.Cfg.Proxmox.Node) {
			return core.Exit(4, "lease_id_conflict: fixed Proxmox lease %s was cloned from another node", leaseID)
		}
		if err := validateFixedProxmoxLocalBinding(claim); err != nil {
			return err
		}
	}
	client, err := newClient(b.Cfg)
	if err != nil {
		return err
	}
	fmt.Fprintf(b.RT.Stderr, "recovering fixed Proxmox lease=%s vmid=%s: requiring cluster absence for %s\n", leaseID, core.Blank(claim.CloudID, "none"), fixedRecoveryAbsenceGrace)
	settled, err := core.SettleUnresolvedFixedAttempt(ctx, fixedProxmoxLeaseKind, claim, func(ctx context.Context, fenced core.LeaseClaim) error {
		evidence, err := proveFixedProxmoxAbsence(ctx, client, fenced, vmid, node)
		if evidence != "" {
			fmt.Fprintf(b.RT.Stderr, "recovery evidence=%s\n", evidence)
		}
		return err
	})
	if err != nil {
		return err
	}
	final, err := core.ReadLeaseClaim(leaseID)
	if err != nil {
		return err
	}
	removeUnboundFixedProxmoxKey(final)
	if !settled {
		fmt.Fprintf(b.RT.Stderr, "lease=%s fixed Proxmox attempt is already settled\n", leaseID)
		return nil
	}
	fmt.Fprintf(b.RT.Stderr, "settled lease=%s fixed Proxmox attempt vmid=%s after cluster absence for %s; the lease ID is spent\n", leaseID, core.Blank(claim.CloudID, "none"), fixedRecoveryAbsenceGrace)
	return nil
}

// A terminal claim without a vmgenid never bound a VM that could use its key.
// Released VMs keep their generation, so their key handling is unchanged.
func removeUnboundFixedProxmoxKey(claim core.LeaseClaim) {
	if fixedProxmoxLeaseKind.IsFixedClaim(claim) && claim.FixedCreateIntent.State == "released" && claim.CloudImmutableID == "" {
		core.RemoveStoredTestboxKey(claim.LeaseID)
	}
}

// proveFixedProxmoxAbsence checks authoritative absence until it has held for
// the whole grace. Any presence, read failure or cancellation ends the proof.
// The durable record is required before settlement and never grants authority.
func proveFixedProxmoxAbsence(ctx context.Context, client proxmoxClient, claim core.LeaseClaim, vmid int, node string) (string, error) {
	data, err := json.MarshalIndent(claim, "", "  ")
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append(data, '\n'))
	started := time.Now()
	record := fixedRecoveryEvidence{
		Version: 1, Kind: fixedRecoveryEvidenceKind, LeaseID: claim.LeaseID, ClaimProvider: claim.Provider,
		ProviderScope: claim.ProviderScope, Fingerprint: claim.FixedCreateIntent.Fingerprint,
		ClaimRevision: claim.Revision, ClaimSHA256: hex.EncodeToString(digest[:]), Attempt: "unsubmitted",
		Grace: fixedRecoveryAbsenceGrace.String(), StartedAt: started.UTC().Format(time.RFC3339Nano),
	}
	if journal := claim.FixedCreateIntent.Journal; journal != nil {
		record.JournalPhase = journal.Phase
	}
	if vmid != 0 {
		record.Attempt, record.VMID, record.Node = "submitted", vmid, node
	}
	var first time.Time
	var proofErr error
	for {
		now := time.Now()
		err := verifyFixedProxmoxAbsence(ctx, client, claim.LeaseID, vmid)
		check := fixedRecoveryCheck{At: now.UTC().Format(time.RFC3339Nano), Absent: err == nil}
		if err != nil {
			check.Error = err.Error()
			record.Checks = append(record.Checks, check)
			proofErr = err
			break
		}
		record.Checks = append(record.Checks, check)
		if first.IsZero() {
			first = now
		}
		if len(record.Checks) >= 2 && now.Sub(first) >= fixedRecoveryAbsenceGrace {
			break
		}
		if err := shared.SleepContext(ctx, fixedRecoveryPollInterval); err != nil {
			proofErr = err
			break
		}
	}
	record.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	record.Outcome = "proven"
	if proofErr != nil {
		record.Outcome, record.Reason = "retained", proofErr.Error()
	}
	path, writeErr := core.WriteFixedRecoveryEvidence(claim.LeaseID, started, record)
	if proofErr != nil {
		return path, errors.Join(fmt.Errorf("retain fixed Proxmox lease %s: absence was not proven for VMID %s: %w", claim.LeaseID, core.Blank(claim.CloudID, "none"), proofErr), writeErr)
	}
	return path, writeErr
}
