package proxmox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/openclaw/crabbox/internal/cli"
)

// The fixtures are the two prepared claims retained after a v0.66.0 clone was
// refused with HTTP 403, with the cluster endpoint, node, repository and slugs
// replaced. The first submitted VMID 102; the second failed while planning
// because the first still bound that VMID.
const (
	retainedSubmittedClaim   = "fixed-lease-v0.66.0-prepared-submitted.json"
	retainedUnsubmittedClaim = "fixed-lease-v0.66.0-prepared-unsubmitted.json"
)

// recoveryClient scripts each absence check and counts clones. It is safe for
// concurrent recoveries.
type recoveryClient struct {
	*fixedProxmoxClient
	mu       sync.Mutex
	lists    int
	lookups  int
	clones   int
	onList   func(call int) ([]core.Server, error)
	onLookup func(call int, id string) (bool, error)
}

func (c *recoveryClient) ListCrabboxServersCluster(context.Context) ([]core.Server, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lists++
	if c.onList != nil {
		return c.onList(c.lists)
	}
	return nil, nil
}

func (c *recoveryClient) VMExistsInCluster(_ context.Context, id string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookups++
	if c.onLookup != nil {
		return c.onLookup(c.lookups, id)
	}
	return false, nil
}

func (c *recoveryClient) CreateServerWithVMID(_ context.Context, _ core.Config, _ string, leaseID, slug string, _ bool, vmid int, labels map[string]string, bind func(core.Server) error) (core.Server, error) {
	c.mu.Lock()
	c.clones++
	c.mu.Unlock()
	server := core.Server{Provider: "proxmox", CloudID: strconv.Itoa(vmid), ID: int64(vmid), HostID: "pve1", ImmutableID: fixedTestGeneration, Name: "crabbox-" + slug, Labels: maps.Clone(labels)}
	server.Labels["lease"], server.Labels["slug"], server.Labels["provider"], server.Labels["crabbox"] = leaseID, slug, "proxmox", "true"
	server.PublicNet.IPv4.IP = "192.0.2.17"
	if err := bind(server); err != nil {
		return core.Server{}, err
	}
	return server, nil
}

func (c *recoveryClient) counts() (lists, lookups, clones int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lists, c.lookups, c.clones
}

func recoveryFixture(t *testing.T) (*leaseBackend, *recoveryClient, core.AcquireRequest) {
	t.Helper()
	backend, fixed, req := fixedProxmoxFixture(t)
	client := &recoveryClient{fixedProxmoxClient: fixed}
	newClient = func(core.Config) (proxmoxClient, error) { return client, nil }
	grace, interval := fixedRecoveryAbsenceGrace, fixedRecoveryPollInterval
	fixedRecoveryAbsenceGrace, fixedRecoveryPollInterval = 30*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { fixedRecoveryAbsenceGrace, fixedRecoveryPollInterval = grace, interval })
	return backend, client, req
}

// installRetainedClaim copies a retained claim into the isolated state and
// creates its per-lease key, as v0.66.0 did before planning.
func installRetainedClaim(t *testing.T, name string, mutate func(*core.LeaseClaim)) (core.LeaseClaim, []byte) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	var claim core.LeaseClaim
	if err := json.Unmarshal(data, &claim); err != nil {
		t.Fatal(err)
	}
	if mutate != nil {
		mutate(&claim)
		if data, err = json.MarshalIndent(claim, "", "  "); err != nil {
			t.Fatal(err)
		}
		data = append(data, '\n')
	}
	writeRecoveryClaim(t, claim.LeaseID, data)
	if _, _, err := core.EnsureTestboxKey(claim.LeaseID); err != nil {
		t.Fatal(err)
	}
	return claim, data
}

func writeRecoveryClaim(t *testing.T, leaseID string, data []byte) {
	t.Helper()
	dir, err := core.CrabboxStateDir()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "claims", leaseID+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func requireRecoveryClaimBytes(t *testing.T, leaseID string, want []byte) {
	t.Helper()
	dir, err := core.CrabboxStateDir()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "claims", leaseID+".json"))
	if err != nil || string(got) != string(want) {
		t.Fatalf("retained claim changed (err=%v):\n%s", err, got)
	}
}

func recoveryEvidence(t *testing.T, leaseID string) []fixedRecoveryEvidence {
	t.Helper()
	stateDir, err := core.CrabboxStateDir()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(stateDir, "fixed-recovery", leaseID)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var records []fixedRecoveryEvidence
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("evidence %s mode=%v err=%v", entry.Name(), info.Mode(), err)
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var record fixedRecoveryEvidence
		if err := json.Unmarshal(data, &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

// cliStopProxmox follows ordinary `crabbox stop`: resolve, then release.
func cliStopProxmox(ctx context.Context, backend *leaseBackend, leaseID string) error {
	lease, err := backend.Resolve(ctx, core.ResolveRequest{ID: leaseID, ReleaseOnly: true})
	if err != nil {
		return err
	}
	return backend.ReleaseLease(ctx, core.ReleaseLeaseRequest{Lease: lease})
}

func requireProvenWindow(t *testing.T, record fixedRecoveryEvidence) {
	t.Helper()
	if record.Outcome != "proven" || record.Reason != "" || len(record.Checks) < 2 {
		t.Fatalf("incomplete proof: %+v", record)
	}
	for _, check := range record.Checks {
		if !check.Absent || check.Error != "" {
			t.Fatalf("proof contains a failed check: %+v", record)
		}
	}
	first, _ := time.Parse(time.RFC3339Nano, record.Checks[0].At)
	last, _ := time.Parse(time.RFC3339Nano, record.Checks[len(record.Checks)-1].At)
	if last.Sub(first) < fixedRecoveryAbsenceGrace {
		t.Fatalf("absence held for %s, want %s", last.Sub(first), fixedRecoveryAbsenceGrace)
	}
}

func TestProxmoxFixedRecoverySettlesRetainedV066Claims(t *testing.T) {
	backend, client, req := recoveryFixture(t)
	ctx := context.Background()
	submitted, submittedBytes := installRetainedClaim(t, retainedSubmittedClaim, nil)
	unsubmitted, _ := installRetainedClaim(t, retainedUnsubmittedClaim, nil)
	if j := submitted.FixedCreateIntent.Journal; j == nil || j.Phase != "submitting" || submitted.CloudID != "102" || submitted.CloudImmutableID != "" {
		t.Fatalf("fixture is not the retained submitted attempt: %+v", submitted)
	}
	if j := unsubmitted.FixedCreateIntent.Journal; j == nil || j.Phase != "prepared" || j.Revision != 1 || len(unsubmitted.FixedCreateIntent.Attempt) != 0 {
		t.Fatalf("fixture is not the retained unsubmitted claim: %+v", unsubmitted)
	}

	// Proxmox keeps offering the free VMID that the retained attempt binds.
	client.nextVMID = 102
	fresh := req
	fresh.RequestedLeaseID, fresh.RequestedSlug = "cbx_aaaaaaaaaaaa", "after-recovery"
	if _, err := backend.Acquire(ctx, fresh); err == nil || !strings.Contains(err.Error(), "multiple local Proxmox claims bind resource 102") {
		t.Fatalf("retained attempt did not block its VMID: %v", err)
	}
	for _, claim := range []core.LeaseClaim{submitted, unsubmitted} {
		if err := backend.ReleaseLease(ctx, core.ReleaseLeaseRequest{Lease: core.LeaseTarget{LeaseID: claim.LeaseID}}); err == nil || !strings.Contains(err.Error(), "lease_id_conflict") {
			t.Fatalf("ordinary stop settled %s: %v", claim.LeaseID, err)
		}
	}

	if err := backend.ReclaimAndStop(ctx, core.StopRequest{ID: submitted.LeaseID}); err != nil {
		t.Fatal(err)
	}
	settled := readFixedProxmoxClaim(t, submitted.LeaseID)
	if settled.FixedCreateIntent.State != "released" || len(settled.FixedCreateIntent.Attempt) != 0 || settled.CloudID != "102" || settled.CloudImmutableID != "" {
		t.Fatalf("recovery did not leave a terminal tombstone: %+v", settled)
	}
	if err := fixedProxmoxLeaseKind.ValidateTerminalClaim(settled, submitted, submitted.LeaseID, validateFixedProxmoxTerminalClaim); err != nil {
		t.Fatal(err)
	}
	requireFixedProxmoxKey(t, submitted.LeaseID, false)
	records := recoveryEvidence(t, submitted.LeaseID)
	digest := sha256.Sum256(submittedBytes)
	if len(records) != 1 || records[0].Attempt != "submitted" || records[0].VMID != 102 || records[0].Node != "pve1" ||
		records[0].JournalPhase != "submitting" || records[0].ClaimRevision != submitted.Revision ||
		records[0].ClaimSHA256 != hex.EncodeToString(digest[:]) || records[0].ProviderScope != submitted.ProviderScope {
		t.Fatalf("evidence does not bind the retained claim: %+v", records)
	}
	requireProvenWindow(t, records[0])

	// Replay of the recovery, ordinary stop and exact acquisition are all fenced by the tombstone.
	lists, lookups, _ := client.counts()
	if err := backend.ReclaimAndStop(ctx, core.StopRequest{ID: submitted.LeaseID}); err != nil {
		t.Fatal(err)
	}
	if l, k, _ := client.counts(); l != lists || k != lookups {
		t.Fatal("replaying a settled recovery contacted Proxmox")
	}
	// Ordinary stop resolves before releasing, even after another lease reuses the VMID.
	client.onList = func(int) ([]core.Server, error) {
		return []core.Server{{Provider: "proxmox", CloudID: "102", HostID: "pve1", Labels: map[string]string{"crabbox": "true", "provider": "proxmox", "lease": "cbx_dddddddddddd"}}}, nil
	}
	if err := cliStopProxmox(ctx, backend, submitted.LeaseID); err != nil {
		t.Fatalf("stop of a settled lease: %v", err)
	}
	client.onList = nil
	replay := req
	replay.RequestedLeaseID, replay.RequestedSlug, replay.Repo.Root = submitted.LeaseID, submitted.Slug, submitted.RepoRoot
	if _, err := backend.Acquire(ctx, replay); err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("recovered lease ID was replayed: %v", err)
	}
	if after := readFixedProxmoxClaim(t, submitted.LeaseID); after.Revision != settled.Revision || len(recoveryEvidence(t, submitted.LeaseID)) != 1 {
		t.Fatal("replay changed the settled claim or added evidence")
	}

	// A claim that never planned a VMID needs only lease identity absence.
	if err := backend.ReclaimAndStop(ctx, core.StopRequest{ID: unsubmitted.LeaseID}); err != nil {
		t.Fatal(err)
	}
	if _, k, _ := client.counts(); k != lookups {
		t.Fatal("unsubmitted recovery looked up a VMID")
	}
	pristine := readFixedProxmoxClaim(t, unsubmitted.LeaseID)
	if pristine.FixedCreateIntent.State != "released" || pristine.CloudID != "" || len(pristine.Labels) != 0 {
		t.Fatalf("unsubmitted recovery left %+v", pristine)
	}
	if err := cliStopProxmox(ctx, backend, unsubmitted.LeaseID); err != nil {
		t.Fatalf("stop rejected an identity-free tombstone: %v", err)
	}
	requireFixedProxmoxKey(t, unsubmitted.LeaseID, false)
	if records := recoveryEvidence(t, unsubmitted.LeaseID); len(records) != 1 || records[0].Attempt != "unsubmitted" || records[0].VMID != 0 {
		t.Fatalf("unsubmitted evidence: %+v", records)
	}

	if client.deleteCalls != 0 || len(client.setLabels) != 0 {
		t.Fatal("recovery or stop of a settled lease mutated Proxmox")
	}

	// The claim left by the earlier conflict now replays onto the freed VMID.
	lease, err := backend.Acquire(ctx, fresh)
	if _, _, clones := client.counts(); err != nil || lease.Server.CloudID != "102" || clones != 1 {
		t.Fatalf("freed VMID was not reusable: err=%v VMID=%s clones=%d", err, lease.Server.CloudID, clones)
	}
}

func TestProxmoxFixedRecoveryRetainsAttemptWithoutContinuousAbsence(t *testing.T) {
	leaseVM := func(label, value string) []core.Server {
		return []core.Server{{Provider: "proxmox", CloudID: "901", HostID: "pve2", Labels: map[string]string{label: value}}}
	}
	for _, tc := range []struct {
		name     string
		onList   func(call int, cancel context.CancelFunc) ([]core.Server, error)
		onLookup func(call int) (bool, error)
		reason   string
	}{
		{name: "VMID reappears", onLookup: func(call int) (bool, error) { return call > 1, nil }, reason: "VMID 102 still exists"},
		{name: "VMID owned by another VM", onLookup: func(int) (bool, error) { return true, nil }, reason: "VMID 102 still exists"},
		{name: "lease VM on another VMID", onList: func(call int, _ context.CancelFunc) ([]core.Server, error) {
			if call > 1 {
				return leaseVM("lease", "cbx_5ead00000001"), nil
			}
			return nil, nil
		}, reason: "surviving VM"},
		{name: "provider key VM", onList: func(call int, _ context.CancelFunc) ([]core.Server, error) {
			if call > 1 {
				return leaseVM("provider_key", "crabbox-cbx-5ead00000001"), nil
			}
			return nil, nil
		}, reason: "surviving VM"},
		{name: "inventory unreadable", onList: func(call int, _ context.CancelFunc) ([]core.Server, error) {
			if call > 1 {
				return nil, errors.New("permission denied: authoritative Proxmox inventory requires propagated VM.Audit on /vms")
			}
			return nil, nil
		}, reason: "VM.Audit"},
		{name: "VMID lookup fails", onLookup: func(int) (bool, error) { return false, errors.New("proxmox GET /cluster/resources: http 500") }, reason: "http 500"},
		{name: "cancelled during grace", onList: func(_ int, cancel context.CancelFunc) ([]core.Server, error) {
			cancel()
			return nil, nil
		}, reason: "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, client, _ := recoveryFixture(t)
			claim, data := installRetainedClaim(t, retainedSubmittedClaim, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.onList != nil {
				client.onList = func(call int) ([]core.Server, error) { return tc.onList(call, cancel) }
			}
			if tc.onLookup != nil {
				client.onLookup = func(call int, _ string) (bool, error) { return tc.onLookup(call) }
			}
			err := backend.ReclaimAndStop(ctx, core.StopRequest{ID: claim.LeaseID})
			if err == nil || !strings.Contains(err.Error(), "retain fixed Proxmox lease") || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("err=%v", err)
			}
			requireRecoveryClaimBytes(t, claim.LeaseID, data)
			requireFixedProxmoxKey(t, claim.LeaseID, true)
			records := recoveryEvidence(t, claim.LeaseID)
			if len(records) != 1 || records[0].Outcome != "retained" || !strings.Contains(records[0].Reason, tc.reason) || len(records[0].Checks) == 0 {
				t.Fatalf("retained proof was not recorded: %+v", records)
			}
			if _, _, clones := client.counts(); clones != 0 || client.deleteCalls != 0 || len(client.setLabels) != 0 {
				t.Fatal("recovery mutated Proxmox")
			}
		})
	}
}

func TestProxmoxFixedRecoveryRefusesIneligibleClaims(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     string
		mutate func(*core.LeaseClaim)
		setup  func(*testing.T, *leaseBackend)
		want   string
	}{
		{name: "slug", id: "fixed-submitted", want: "exact canonical lease ID"},
		{name: "missing claim", id: "cbx_bbbbbbbbbbbb", want: "has no fixed Proxmox claim"},
		{name: "ordinary claim", mutate: func(c *core.LeaseClaim) { c.Provider, c.FixedCreateIntent = "proxmox", nil }, want: "has no fixed Proxmox claim"},
		{name: "other cluster", setup: func(_ *testing.T, b *leaseBackend) { b.Cfg.Proxmox.APIURL = "https://other.example.test:8006" }, want: "another cluster scope"},
		{name: "bound generation", mutate: func(c *core.LeaseClaim) { c.CloudImmutableID = fixedTestGeneration }, want: "bound or deleting"},
		{name: "acquired", mutate: func(c *core.LeaseClaim) {
			c.CloudImmutableID, c.FixedCreateIntent.State = fixedTestGeneration, "acquired"
		}, want: "bound or deleting"},
		{name: "deleting", mutate: func(c *core.LeaseClaim) {
			c.CloudImmutableID, c.FixedCreateIntent.State = fixedTestGeneration, "deleting"
		}, want: "bound or deleting"},
		{name: "attempt identity mismatch", mutate: func(c *core.LeaseClaim) { c.CloudID, c.CloudNumericID = "103", 103 }, want: "inconsistent durable identity"},
		{name: "failed attempts", mutate: func(c *core.LeaseClaim) { c.FixedCreateIntent.FailedAttempts = []string{"101"} }, want: "invalid fixed Proxmox identity"},
		{name: "runtime adapter owner", mutate: func(c *core.LeaseClaim) { c.RuntimeAdapterRegistrationID = "adapter-registration" }, want: "without another owner"},
		{name: "another local owner", setup: func(t *testing.T, _ *leaseBackend) {
			installRetainedClaim(t, retainedSubmittedClaim, func(c *core.LeaseClaim) {
				c.LeaseID, c.Labels["lease"], c.Labels["provider_key"] = "cbx_cccccccccccc", "cbx_cccccccccccc", "crabbox-cbx-cccccccccccc"
			})
		}, want: "multiple local Proxmox claims bind resource 102"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, client, _ := recoveryFixture(t)
			claim, data := installRetainedClaim(t, retainedSubmittedClaim, tc.mutate)
			if tc.setup != nil {
				tc.setup(t, backend)
			}
			id := claim.LeaseID
			if tc.id != "" {
				id = tc.id
			}
			if err := backend.ReclaimAndStop(context.Background(), core.StopRequest{ID: id}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v, want %q", err, tc.want)
			}
			requireRecoveryClaimBytes(t, claim.LeaseID, data)
			requireFixedProxmoxKey(t, claim.LeaseID, true)
			if lists, lookups, clones := client.counts(); lists != 0 || lookups != 0 || clones != 0 || len(recoveryEvidence(t, claim.LeaseID)) != 0 {
				t.Fatalf("ineligible claim reached Proxmox or evidence: lists=%d lookups=%d clones=%d", lists, lookups, clones)
			}
		})
	}
}

func TestProxmoxFixedRecoveryInterruptedBeforeSettlementRequiresFreshProof(t *testing.T) {
	backend, client, _ := recoveryFixture(t)
	fixedRecoveryAbsenceGrace = 0
	claim, data := installRetainedClaim(t, retainedSubmittedClaim, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Stop after the final check has passed and the proof is durable.
	client.onLookup = func(call int, _ string) (bool, error) {
		if call == 2 {
			cancel()
		}
		return false, nil
	}
	if err := backend.ReclaimAndStop(ctx, core.StopRequest{ID: claim.LeaseID}); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted settlement err=%v", err)
	}
	requireRecoveryClaimBytes(t, claim.LeaseID, data)
	requireFixedProxmoxKey(t, claim.LeaseID, true)
	if records := recoveryEvidence(t, claim.LeaseID); len(records) != 1 || records[0].Outcome != "proven" {
		t.Fatalf("proof before interruption: %+v", records)
	}

	// The durable proof grants nothing: a restart proves absence again.
	client.onLookup = nil
	_, before, _ := client.counts()
	if err := backend.ReclaimAndStop(context.Background(), core.StopRequest{ID: claim.LeaseID}); err != nil {
		t.Fatal(err)
	}
	if _, after, _ := client.counts(); after-before < 2 {
		t.Fatalf("restart reused the earlier proof: %d new VMID checks", after-before)
	}
	records := recoveryEvidence(t, claim.LeaseID)
	if len(records) != 2 || records[1].StartedAt == records[0].StartedAt {
		t.Fatalf("restart did not record a fresh proof: %+v", records)
	}
	requireProvenWindow(t, records[1])
	if settled := readFixedProxmoxClaim(t, claim.LeaseID); settled.FixedCreateIntent.State != "released" {
		t.Fatalf("restart did not settle: %+v", settled)
	}
	requireFixedProxmoxKey(t, claim.LeaseID, false)
}

func TestProxmoxFixedRecoveryFencesConcurrentRecoveryAndReplay(t *testing.T) {
	backend, client, req := recoveryFixture(t)
	fixedRecoveryAbsenceGrace = 100 * time.Millisecond
	claim, _ := installRetainedClaim(t, retainedSubmittedClaim, nil)
	inside := make(chan struct{})
	var once sync.Once
	client.onList = func(int) ([]core.Server, error) {
		once.Do(func() { close(inside) })
		return nil, nil
	}
	errs := make(chan error, 3)
	for range 2 {
		go func() { errs <- backend.ReclaimAndStop(context.Background(), core.StopRequest{ID: claim.LeaseID}) }()
	}
	<-inside
	// An exact replay waits for the recovery fence and then finds the tombstone.
	replay := req
	replay.RequestedLeaseID, replay.RequestedSlug, replay.Repo.Root = claim.LeaseID, claim.Slug, claim.RepoRoot
	go func() {
		_, err := backend.Acquire(context.Background(), replay)
		errs <- err
	}()
	var terminal int
	for range 3 {
		if err := <-errs; err != nil {
			if !strings.Contains(err.Error(), "terminal") {
				t.Fatal(err)
			}
			terminal++
		}
	}
	if terminal != 1 {
		t.Fatalf("replay outcomes: %d terminal", terminal)
	}
	if records := recoveryEvidence(t, claim.LeaseID); len(records) != 1 {
		t.Fatalf("concurrent recoveries proved %d times", len(records))
	}
	if settled := readFixedProxmoxClaim(t, claim.LeaseID); settled.FixedCreateIntent.State != "released" {
		t.Fatalf("claim=%+v", settled)
	}
	if _, _, clones := client.counts(); clones != 0 {
		t.Fatal("replay cloned during recovery")
	}
}

func TestProxmoxFixedRecoveryRequiresDurableEvidence(t *testing.T) {
	backend, _, _ := recoveryFixture(t)
	claim, data := installRetainedClaim(t, retainedSubmittedClaim, nil)
	stateDir, err := core.CrabboxStateDir()
	if err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(stateDir, "fixed-recovery", claim.LeaseID)
	if err := os.MkdirAll(filepath.Dir(blocked), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocked, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := backend.ReclaimAndStop(context.Background(), core.StopRequest{ID: claim.LeaseID}); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("settled without durable evidence: %v", err)
	}
	requireRecoveryClaimBytes(t, claim.LeaseID, data)
	requireFixedProxmoxKey(t, claim.LeaseID, true)
}
