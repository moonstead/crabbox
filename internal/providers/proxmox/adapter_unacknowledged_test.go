package proxmox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/openclaw/crabbox/internal/cli"
)

func adapterAttemptLeaseID(t *testing.T, dir, id string) string {
	t.Helper()
	leaseID, _ := adapterAttempt(t, dir, id)
	return leaseID
}

// adapterAttempt returns an unacknowledged workspace's attempt ID and the time
// its creation was prepared, when the late-creation window starts.
func adapterAttempt(t *testing.T, dir, id string) (string, time.Time) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "adapter-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state struct {
		Workspaces map[string]struct {
			AttemptLeaseID   string    `json:"attemptLeaseId"`
			LeaseID          string    `json:"leaseId"`
			CreatePreparedAt time.Time `json:"createPreparedAt"`
		} `json:"workspaces"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	record := state.Workspaces[id]
	if !core.IsCanonicalLeaseID(record.AttemptLeaseID) || record.LeaseID != "" {
		t.Fatalf("workspace %s identity=%+v", id, record)
	}
	return record.AttemptLeaseID, record.CreatePreparedAt
}

// adapterCleanupRefused waits for repeated confirmed-absence cleanup attempts.
// Without an acknowledged identity the adapter runs no other stop child.
func adapterCleanupRefused(times int) func(map[string]any, adapterChildState) bool {
	return func(workspace map[string]any, state adapterChildState) bool {
		stops := 0
		for _, command := range state.Commands {
			if command == "stop" {
				stops++
			}
		}
		return workspace["status"] == "stopping" && stops >= times
	}
}

func adapterTestBinary(t *testing.T) string {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return binary
}

// An uncertain clone leaves a prepared claim that adapter cleanup must never
// settle. After an operator releases the attempt with the checked stop --force
// recovery, the adapter trusts that attempt's released receipt and finishes.
func TestProxmoxAdapterSettlesUnacknowledgedAttemptOnlyFromReleasedReceipt(t *testing.T) {
	setControllerTestEnv(t, controllerTestConfig())
	dir := t.TempDir()
	inventory := filepath.Join(dir, "proxmox-inventory.json")
	other := adapterChildVM{VMID: 102, Node: "pve1", Name: "crabbox-other-box", Generation: replacementGeneration, Labels: map[string]string{
		"crabbox": "true", "provider": "proxmox", "lease": "cbx_aaaaaaaaaaaa", "slug": "other-box", "state": "ready",
	}}
	if err := writeAdapterChildState(inventory, adapterChildState{VMs: []adapterChildVM{other}, CloneFailure: "uncertain"}); err != nil {
		t.Fatal(err)
	}
	// A short create timeout ends the late-creation window quickly.
	adapter := startProxmoxAdapter(t, dir, inventory, "--create-timeout", "5s")
	if code, body := adapter.call(http.MethodPost, "/v1/workspaces", map[string]any{"id": "uncertain-clone"}); code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%v", code, body)
	}
	_, state := adapter.wait("uncertain-clone", time.Minute, adapterCleanupRefused(2))
	leaseID := adapterAttemptLeaseID(t, dir, "uncertain-clone")
	prepared := readFixedProxmoxClaim(t, leaseID)
	if prepared.FixedCreateIntent.State != "prepared" || prepared.CloudID != "417" || prepared.CloudImmutableID != "" {
		t.Fatalf("uncertain clone claim=%+v", prepared)
	}
	if !slices.ContainsFunc(state.Errors, func(message string) bool { return strings.Contains(message, "invalid terminal tombstone") }) {
		t.Fatalf("prepared claim was not refused as a receipt: %q", state.Errors)
	}
	if err := exec.CommandContext(t.Context(), adapterTestBinary(t), "stop", "--force", "--provider", "proxmox", "--id", leaseID).Run(); err != nil {
		t.Fatalf("operator stop --force: %v", err)
	}
	receipt := readFixedProxmoxClaim(t, leaseID)
	if receipt.FixedCreateIntent.State != "released" {
		t.Fatalf("operator recovery left claim=%+v", receipt)
	}
	workspace, state := adapter.wait("uncertain-clone", time.Minute, adapterWorkspaceSettled)
	if err := adapter.stop(); err != nil {
		t.Fatalf("adapter serve: %v", err)
	}
	if workspace["status"] != "failed" || workspace["message"] != "workspace provisioning failed before provider identity acknowledgment" {
		t.Fatalf("terminal workspace=%v", workspace)
	}
	if after := readFixedProxmoxClaim(t, leaseID); !reflect.DeepEqual(after, receipt) {
		t.Fatalf("cleanup changed the receipt: %+v", after)
	}
	if state.Clones != 1 || len(state.Deleted) != 0 || !reflect.DeepEqual(state.VMs, []adapterChildVM{other}) {
		t.Fatalf("clones=%d deleted=%v remaining=%+v", state.Clones, state.Deleted, state.VMs)
	}
}

// The adapter's cleanup child for an attempt it never acknowledged, after an
// operator's ordinary stop released it: only the released receipt of exactly
// that attempt, slug and scope, never registered with a coordinator, matches.
func TestProxmoxUnacknowledgedCleanupMatchesOnlyExactReleasedAttempt(t *testing.T) {
	_, client, _ := fixedProxmoxFixture(t)
	cfg := controllerTestConfig()
	setControllerTestEnv(t, cfg)
	scope := controllerTestScope(t, cfg)
	t.Setenv("CRABBOX_ADAPTER_PROVIDER_SCOPE", scope)
	waitForSSHReadyFunc = func(context.Context, *core.SSHTarget, io.Writer, string, time.Duration) error {
		return errors.New("bootstrap failed")
	}
	const leaseID, slug = "cbx_0123456789ab", "adapter-box"
	run := func(args ...string) error {
		var stdout, stderr bytes.Buffer
		return (core.App{Stdout: &stdout, Stderr: &stderr}).Run(t.Context(), args)
	}
	cleanupInScope := func(scope, attempt, slug, lease, resource string) error {
		return run("stop", "--confirmed-absent-local-cleanup=true", "--id", attempt,
			"--expected-provider-lease-id", lease, "--expected-provider-attempt-lease-id", attempt,
			"--expected-provider-slug", slug, "--expected-provider-resource-id", resource, "--expected-provider-scope", scope,
			"--expected-coordinator-registration-url", "", "--provider", "proxmox")
	}
	cleanup := func(attempt, slug, lease, resource string) error {
		return cleanupInScope(scope, attempt, slug, lease, resource)
	}
	if err := run("warmup", "--keep=true", "--lease-id", leaseID, "--slug", slug, "--provider", "proxmox"); err == nil {
		t.Fatal("bootstrap failure was not reported")
	}
	prepared := readFixedProxmoxClaim(t, leaseID)
	if err := cleanup(leaseID, slug, "", ""); err == nil || !reflect.DeepEqual(prepared, readFixedProxmoxClaim(t, leaseID)) {
		t.Fatalf("cleanup settled a prepared claim: %v", err)
	}
	// Release reads provider state only; keep best-effort guest cleanup offline.
	client.servers[0].PublicNet.IPv4.IP = ""
	if err := run("stop", "--provider", "proxmox", "--id", leaseID); err != nil {
		t.Fatalf("operator stop: %v", err)
	}
	receipt := readFixedProxmoxClaim(t, leaseID)
	if receipt.FixedCreateIntent.State != "released" || client.deleteCalls != 1 {
		t.Fatalf("operator stop deletes=%d receipt=%+v", client.deleteCalls, receipt)
	}
	// An attempt with no claim at all has nothing to keep, and its cleanup
	// cannot touch another attempt's receipt.
	if err := cleanup("cbx_abcdefabcdef", slug, "", ""); err != nil {
		t.Fatalf("unclaimed attempt: %v", err)
	}
	if _, exists, err := core.ReadLeaseClaimWithPresence("cbx_abcdefabcdef"); err != nil || exists {
		t.Fatalf("unclaimed cleanup wrote a claim: exists=%t err=%v", exists, err)
	}
	for name, err := range map[string]error{
		"other slug":       cleanup(leaseID, "other-box", "", ""),
		"lease without VM": cleanup(leaseID, slug, leaseID, ""),
		"VM without lease": cleanup(leaseID, slug, "", "417"),
		"other scope":      cleanupInScope(proxmoxControllerScopePrefix+strings.Repeat("0", 64), leaseID, slug, "", ""),
	} {
		if err == nil {
			t.Fatalf("%s: cleanup accepted", name)
		}
	}
	replaceClaim := func(claim core.LeaseClaim) {
		t.Helper()
		if err := core.WithDurableLeaseClaimLock(leaseID, func(current *core.LeaseClaim, _ bool, persist func() error) error {
			*current = claim
			return persist()
		}); err != nil {
			t.Fatal(err)
		}
	}
	for name, mutate := range map[string]func(*core.LeaseClaim){
		"registered":           func(c *core.LeaseClaim) { c.RuntimeAdapterRegistrationID = "registration-1" },
		"pending registration": func(c *core.LeaseClaim) { c.RuntimeAdapterPendingRegistrationID = "registration-1" },
		"claim scope": func(c *core.LeaseClaim) {
			c.ProviderScope = "endpoint:https://pve.example.test:8006|node:pve2"
			c.FixedCreateIntent.ProviderScope = c.ProviderScope
		},
	} {
		mismatched := core.CloneLeaseClaim(receipt)
		mutate(&mismatched)
		replaceClaim(mismatched)
		if err := cleanup(leaseID, slug, "", ""); err == nil {
			t.Fatalf("%s receipt: cleanup accepted", name)
		}
	}
	replaceClaim(receipt)
	// An unreadable claim is never treated as a missing one.
	stateDir, err := core.CrabboxStateDir()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(stateDir, "claims", leaseID+".json")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(leaseID, slug, "", ""); err == nil {
		t.Fatal("unreadable claim: cleanup accepted")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "{" {
		t.Fatalf("cleanup changed an unreadable claim: %q %v", data, err)
	}
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	receipt = readFixedProxmoxClaim(t, leaseID)
	for attempt := range 2 {
		if err := cleanup(leaseID, slug, "", ""); err != nil {
			t.Fatalf("cleanup attempt %d: %v", attempt+1, err)
		}
	}
	if after := readFixedProxmoxClaim(t, leaseID); !reflect.DeepEqual(after, receipt) || client.deleteCalls != 1 {
		t.Fatalf("cleanup changed the receipt or Proxmox: deletes=%d receipt=%+v", client.deleteCalls, after)
	}
}

// adapterFirstCleanup returns the index of the first confirmed-absence cleanup
// child and checks that no cleanup child created a Proxmox API client.
func adapterFirstCleanup(t *testing.T, outcomes []adapterChildOutcome) int {
	t.Helper()
	first := -1
	for i, outcome := range outcomes {
		if outcome.Command == "stop" && !outcome.Cleanup {
			t.Fatalf("unacknowledged attempt ran a provider stop: %+v", outcomes)
		}
		if outcome.Cleanup {
			if outcome.Clients != 0 {
				t.Fatalf("cleanup contacted the Proxmox API: %+v", outcome)
			}
			if first < 0 {
				first = i
			}
		}
	}
	if first < 0 {
		t.Fatalf("no cleanup child ran: %+v", outcomes)
	}
	return first
}

func newUnclaimedAdapterTest(t *testing.T, state adapterChildState, registered bool) (string, *proxmoxAdapterHarness, func() []string) {
	t.Helper()
	setControllerTestEnv(t, controllerTestConfig())
	var mu sync.Mutex
	var calls []string
	if registered {
		coordinator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			calls = append(calls, r.Method+" "+r.URL.Path)
			mu.Unlock()
			http.Error(w, `{"error":"not_found"}`, http.StatusNotFound)
		}))
		t.Cleanup(coordinator.Close)
		for key, value := range map[string]string{
			"CRABBOX_COORDINATOR": coordinator.URL, "CRABBOX_COORDINATOR_TOKEN": "coordinator-test-token",
			"CRABBOX_COORDINATOR_MODE": "registered", "CRABBOX_ADAPTER_ID": "proxmox-lab",
		} {
			t.Setenv(key, value)
		}
	}
	dir := t.TempDir()
	inventory := filepath.Join(dir, "proxmox-inventory.json")
	state.VMs = []adapterChildVM{unclaimedTestOtherVM}
	if err := writeAdapterChildState(inventory, state); err != nil {
		t.Fatal(err)
	}
	// A short create timeout ends the late-creation window quickly.
	adapter := startProxmoxAdapter(t, dir, inventory, "--create-timeout", unclaimedTestWindow.String())
	if code, body := adapter.call(http.MethodPost, "/v1/workspaces", map[string]any{"id": "unclaimed"}); code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%v", code, body)
	}
	return dir, adapter, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(calls)
	}
}

const unclaimedTestWindow = 5 * time.Second

var unclaimedTestOtherVM = adapterChildVM{VMID: 102, Node: "pve1", Name: "crabbox-other-box", Generation: replacementGeneration, Labels: map[string]string{
	"crabbox": "true", "provider": "proxmox", "lease": "cbx_aaaaaaaaaaaa", "slug": "other-box", "state": "ready",
}}

// finishUnclaimedAdapterTest checks the terminal workspace and that cleanup
// wrote, deleted and adopted nothing.
func finishUnclaimedAdapterTest(t *testing.T, dir string, adapter *proxmoxAdapterHarness, workspace map[string]any, state adapterChildState, clones int) (int, time.Time) {
	t.Helper()
	if err := adapter.stop(); err != nil {
		t.Fatalf("adapter serve: %v", err)
	}
	if workspace["status"] != "failed" || workspace["message"] != "workspace provisioning failed before provider identity acknowledgment" {
		t.Fatalf("terminal workspace=%v", workspace)
	}
	leaseID, preparedAt := adapterAttempt(t, dir, "unclaimed")
	if _, exists, err := core.ReadLeaseClaimWithPresence(leaseID); err != nil || exists {
		t.Fatalf("cleanup wrote a claim: exists=%t err=%v", exists, err)
	}
	if state.Clones != clones || len(state.Deleted) != 0 || !reflect.DeepEqual(state.VMs, []adapterChildVM{unclaimedTestOtherVM}) {
		t.Fatalf("clones=%d deleted=%v remaining=%+v", state.Clones, state.Deleted, state.VMs)
	}
	first := adapterFirstCleanup(t, state.Outcomes)
	if windowEnd := preparedAt.Add(unclaimedTestWindow); state.Outcomes[first].At.Before(windowEnd) {
		t.Fatalf("cleanup at %s ran before the late-creation window ended at %s", state.Outcomes[first].At, windowEnd)
	}
	return first, preparedAt
}

// A definite clone rejection removes the attempt's claim so that the fixed ID
// can be retried, and an unreachable API fails before the claim is written.
// Neither attempt has a VM. After the late-creation window and stable absence
// the adapter finishes the workspace. Cleanup writes, deletes and adopts
// nothing and contacts neither Proxmox nor the coordinator.
func TestProxmoxAdapterSettlesUnclaimedAttemptItNeverAcknowledged(t *testing.T) {
	for _, failure := range []string{"rejected", "unreachable"} {
		for _, registered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/registered=%t", failure, registered), func(t *testing.T) {
				dir, adapter, coordinatorCalls := newUnclaimedAdapterTest(t, adapterChildState{CloneFailure: failure}, registered)
				workspace, state := adapter.wait("unclaimed", time.Minute, adapterWorkspaceSettled)
				clones := map[string]int{"rejected": 1, "unreachable": 0}[failure]
				first, _ := finishUnclaimedAdapterTest(t, dir, adapter, workspace, state, clones)
				lists := 0
				for _, outcome := range state.Outcomes[:first] {
					if outcome.Command == "list" {
						if outcome.Failed {
							t.Fatalf("inventory read failed: %+v", state.Outcomes)
						}
						lists++
					}
				}
				if lists < 2 {
					t.Fatalf("cleanup followed %d absence checks: %+v", lists, state.Outcomes)
				}
				if calls := coordinatorCalls(); len(calls) != 0 {
					t.Fatalf("unregistered attempt contacted the coordinator: %v", calls)
				}
				data, err := os.ReadFile(filepath.Join(dir, "adapter-state.json"))
				if err != nil {
					t.Fatal(err)
				}
				var adapterState struct {
					Workspaces map[string]struct {
						CoordinatorRegistrationURL string `json:"coordinatorRegistrationUrl"`
					} `json:"workspaces"`
				}
				if err := json.Unmarshal(data, &adapterState); err != nil {
					t.Fatal(err)
				}
				if bound := adapterState.Workspaces["unclaimed"].CoordinatorRegistrationURL; (bound != "") != registered {
					t.Fatalf("registered=%t but workspace coordinator binding=%q", registered, bound)
				}
			})
		}
	}
}

// An inventory read error is never absence. Read errors that interrupt an
// observed absence and continue past the late-creation window restart the
// stable-absence proof, and cleanup waits for it.
func TestProxmoxAdapterUnclaimedCleanupWaitsForStableAbsence(t *testing.T) {
	errorsUntil := time.Now().Add(14 * time.Second)
	dir, adapter, _ := newUnclaimedAdapterTest(t, adapterChildState{CloneFailure: "rejected", ListErrorsAfter: 1, ListErrorsUntil: errorsUntil}, false)
	workspace, state := adapter.wait("unclaimed", time.Minute, adapterWorkspaceSettled)
	first, preparedAt := finishUnclaimedAdapterTest(t, dir, adapter, workspace, state, 1)
	firstError, lastError := -1, -1
	for i, outcome := range state.Outcomes {
		if outcome.Command == "list" && outcome.Failed {
			if firstError < 0 {
				firstError = i
			}
			lastError = i
		}
	}
	if firstError < 0 || !slices.ContainsFunc(state.Outcomes[:firstError], func(outcome adapterChildOutcome) bool {
		return outcome.Command == "list" && !outcome.Failed
	}) {
		t.Fatalf("read errors did not interrupt an observed absence: %+v", state.Outcomes)
	}
	if state.Outcomes[lastError].At.Before(preparedAt.Add(unclaimedTestWindow)) {
		t.Fatalf("read errors ended before the late-creation window: %+v", state.Outcomes)
	}
	if first < lastError {
		t.Fatalf("cleanup ran before read errors stopped: %+v", state.Outcomes)
	}
	resumed := slices.IndexFunc(state.Outcomes[lastError:], func(outcome adapterChildOutcome) bool { return outcome.Command == "list" && !outcome.Failed })
	// The adapter's default retry delay separates two absence observations.
	if resumed < 0 || state.Outcomes[first].At.Before(state.Outcomes[lastError+resumed].At.Add(2*time.Second)) {
		t.Fatalf("cleanup did not wait for stable absence after read errors: %+v", state.Outcomes)
	}
}
