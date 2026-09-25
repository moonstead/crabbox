package proxmox

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	core "github.com/openclaw/crabbox/internal/cli"
)

// The fixtures are the two prepared claims retained after a v0.66.0 clone was
// refused with HTTP 403, with the cluster endpoint, node, repository and slugs
// replaced. The first submitted VMID 102; the second failed while planning
// because the first still bound that VMID.
func installRetainedV066Claim(t *testing.T, name string) (core.LeaseClaim, []byte) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	var claim core.LeaseClaim
	if err := json.Unmarshal(data, &claim); err != nil {
		t.Fatal(err)
	}
	dir, err := core.CrabboxStateDir()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "claims", claim.LeaseID+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.EnsureTestboxKey(claim.LeaseID); err != nil {
		t.Fatal(err)
	}
	return claim, data
}

func retainedV066ClaimBytes(t *testing.T, leaseID string) []byte {
	t.Helper()
	dir, err := core.CrabboxStateDir()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "claims", leaseID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func retainedV066KeyExists(t *testing.T, leaseID string) bool {
	t.Helper()
	path, err := core.TestboxKeyPath(leaseID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = os.Stat(path)
	return err == nil
}

// A crash between a separate deleting record and the tombstone would leave an
// unbound deleting claim that neither stop, stop --force nor replay can settle.
func TestProxmoxForceRecoveryOfRetainedV066AttemptWritesTombstoneOnce(t *testing.T) {
	backend, client, _ := fixedProxmoxFixture(t)
	claim, _ := installRetainedV066Claim(t, "fixed-lease-v0.66.0-prepared-submitted.json")
	if err := backend.ReclaimAndStop(context.Background(), core.StopRequest{ID: claim.LeaseID}); err != nil {
		t.Fatal(err)
	}
	after := readFixedProxmoxClaim(t, claim.LeaseID)
	if after.FixedCreateIntent.State != "released" || after.CloudID != "102" || after.FixedCreateIntent.Journal.Revision != claim.FixedCreateIntent.Journal.Revision+1 {
		t.Fatalf("recovery did not settle in one claim write: before=%+v after=%+v", claim.FixedCreateIntent.Journal, after.FixedCreateIntent.Journal)
	}
	if retainedV066KeyExists(t, claim.LeaseID) || client.deleteCalls != 0 || client.fixedCreates != 0 {
		t.Fatal("recovery kept the key or mutated Proxmox")
	}
	// Another lease reusing the VMID must not make ordinary stop of the receipt fail.
	client.servers = []core.Server{{Provider: "proxmox", CloudID: "102", HostID: "pve1", Labels: map[string]string{"crabbox": "true", "provider": "proxmox", "lease": "cbx_dddddddddddd"}}}
	client.getServerByID = map[string]core.Server{"102": client.servers[0]}
	if err := stopLikeCLI(backend, claim.LeaseID); err != nil || client.deleteCalls != 0 {
		t.Fatalf("stop of a recovered receipt after VMID reuse: err=%v deletes=%d", err, client.deleteCalls)
	}
}

// stopLikeCLI follows ordinary `crabbox stop`: resolve, then release.
func stopLikeCLI(backend *leaseBackend, leaseID string) error {
	lease, err := backend.Resolve(context.Background(), core.ResolveRequest{ID: leaseID, ReleaseOnly: true})
	if err != nil {
		return err
	}
	return backend.ReleaseLease(context.Background(), core.ReleaseLeaseRequest{Lease: lease})
}

func TestProxmoxForceRecoverySettlesRetainedV066UnplannedIntent(t *testing.T) {
	for _, scenario := range []string{"absent", "lease present", "provider key present", "inventory error", "active clone"} {
		t.Run(scenario, func(t *testing.T) {
			backend, client, _ := fixedProxmoxFixture(t)
			claim, data := installRetainedV066Claim(t, "fixed-lease-v0.66.0-prepared-unsubmitted.json")
			if len(claim.FixedCreateIntent.Attempt) != 0 || claim.CloudID != "" {
				t.Fatalf("fixture is not an unplanned intent: %+v", claim)
			}
			switch scenario {
			case "lease present":
				client.servers = []core.Server{{CloudID: "418", Labels: map[string]string{"lease": claim.LeaseID}}}
			case "provider key present":
				client.servers = []core.Server{{CloudID: "418", Labels: map[string]string{"provider_key": core.ProviderKeyForLease(claim.LeaseID)}}}
			case "inventory error":
				client.clusterListErr = errors.New("inventory unavailable")
			case "active clone":
				client.activeCloneErr = errors.New("clone task still active")
			}
			err := backend.ReclaimAndStop(context.Background(), core.StopRequest{ID: claim.LeaseID})
			if scenario != "absent" {
				if err == nil || string(retainedV066ClaimBytes(t, claim.LeaseID)) != string(data) || !retainedV066KeyExists(t, claim.LeaseID) {
					t.Fatalf("unproven recovery changed custody: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			settled := readFixedProxmoxClaim(t, claim.LeaseID)
			if settled.FixedCreateIntent.State != "released" || settled.CloudID != "" || len(settled.Labels) != 0 || settled.FixedCreateIntent.Journal.Revision != 2 {
				t.Fatalf("unplanned intent was not settled in one write: %+v", settled)
			}
			if retainedV066KeyExists(t, claim.LeaseID) {
				t.Fatal("recovery kept the unused key")
			}
			if err := backend.ReclaimAndStop(context.Background(), core.StopRequest{ID: claim.LeaseID}); err != nil {
				t.Fatalf("terminal force retry: %v", err)
			}
			if err := stopLikeCLI(backend, claim.LeaseID); err != nil {
				t.Fatalf("stop rejected the identity-free tombstone: %v", err)
			}
			if after := readFixedProxmoxClaim(t, claim.LeaseID); after.Revision != settled.Revision {
				t.Fatal("retries changed the tombstone")
			}
			if client.deleteCalls != 0 || client.fixedCreates != 0 || !strings.Contains(settled.FixedCreateIntent.Fingerprint, "43407d21") {
				t.Fatal("recovery mutated Proxmox or lost the intent")
			}
		})
	}
}

func TestProxmoxForceRecoveryOfUnplannedIntentThroughHTTPAPI(t *testing.T) {
	backend, _, _ := fixedProxmoxFixture(t)
	claim, _ := installRetainedV066Claim(t, "fixed-lease-v0.66.0-prepared-unsubmitted.json")
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data any
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api2/json/access/permissions":
			data = map[string]any{r.URL.Query().Get("path"): map[string]int{"VM.Audit": 1, "Sys.Audit": 1}}
		case r.Method == http.MethodGet && r.URL.Path == "/api2/json/cluster/resources":
			data = []any{}
		case r.Method == http.MethodGet && r.URL.Path == "/api2/json/nodes/pve1/tasks":
			data = []any{}
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	t.Cleanup(api.Close)
	backend.Cfg.Proxmox.APIURL = api.URL
	newClient = func(cfg core.Config) (proxmoxClient, error) { return core.NewProxmoxClient(cfg) }
	// The retained claim's scope names the fixture endpoint; bind it to this server.
	scoped := readFixedProxmoxClaim(t, claim.LeaseID)
	scoped.ProviderScope = core.ProviderClaimScope("proxmox", backend.Cfg)
	scoped.FixedCreateIntent.ProviderScope = scoped.ProviderScope
	if _, err := core.ReplaceLeaseClaimIfUnchangedDurableReturning(claim.LeaseID, readFixedProxmoxClaim(t, claim.LeaseID), scoped); err != nil {
		t.Fatal(err)
	}
	if err := backend.ReclaimAndStop(context.Background(), core.StopRequest{ID: claim.LeaseID}); err != nil {
		t.Fatal(err)
	}
	if settled := readFixedProxmoxClaim(t, claim.LeaseID); settled.FixedCreateIntent.State != "released" || settled.CloudID != "" {
		t.Fatalf("unplanned intent was not settled: %+v", settled)
	}
}
