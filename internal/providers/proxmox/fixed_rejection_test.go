package proxmox

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	core "github.com/openclaw/crabbox/internal/cli"
)

func proxmoxCloneRejection() error {
	return &core.ProxmoxCloneRejectedError{Err: &core.ProxmoxError{
		Method: http.MethodPost, Path: "/nodes/pve1/qemu/9400/clone", StatusCode: http.StatusForbidden,
		Body: "Permission check failed (/sdn/zones/localnetwork/vmbr0, SDN.Use)",
	}}
}

func requireFixedProxmoxKey(t *testing.T, leaseID string, want bool) {
	t.Helper()
	path, err := core.TestboxKeyPath(leaseID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = os.Stat(path)
	if exists := err == nil; exists != want {
		t.Fatalf("lease key exists=%t, want %t (stat error %v)", exists, want, err)
	}
}

func TestProxmoxFixedDefiniteCloneRejectionLeavesTerminalTombstone(t *testing.T) {
	backend, client, req := fixedProxmoxFixture(t)
	client.fixedCreateErr = proxmoxCloneRejection()
	_, err := backend.Acquire(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "lease_id_conflict") || !strings.Contains(err.Error(), "terminated") || !strings.Contains(err.Error(), "SDN.Use") {
		t.Fatalf("rejection err=%v", err)
	}
	claim := readFixedProxmoxClaim(t, req.RequestedLeaseID)
	if claim.FixedCreateIntent.State != "released" || len(claim.FixedCreateIntent.Attempt) != 0 || claim.CloudID != "417" || claim.CloudImmutableID != "" {
		t.Fatalf("rejected clone did not leave a terminal tombstone: %+v", claim)
	}
	requireFixedProxmoxKey(t, req.RequestedLeaseID, false)

	// The lease ID is spent; neither replay nor stop may clone or delete anything.
	if _, err := backend.Acquire(context.Background(), req); err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("rejected lease ID was replayed: %v", err)
	}
	if err := backend.ReleaseLease(context.Background(), core.ReleaseLeaseRequest{Lease: core.LeaseTarget{LeaseID: req.RequestedLeaseID}}); err != nil {
		t.Fatalf("stop of rejected lease: %v", err)
	}
	if after := readFixedProxmoxClaim(t, req.RequestedLeaseID); !reflect.DeepEqual(claim, after) {
		t.Fatal("stop changed the terminal tombstone")
	}
	if client.fixedCreates != 1 || client.deleteCalls != 0 {
		t.Fatalf("clones=%d deletes=%d", client.fixedCreates, client.deleteCalls)
	}

	// Proxmox keeps offering the lowest free VMID; the tombstone must not block it.
	client.fixedCreateErr = nil
	req.RequestedLeaseID, req.RequestedSlug = "cbx_aaaaaaaaaaaa", "after-rejection"
	lease, err := backend.Acquire(context.Background(), req)
	if err != nil || lease.Server.CloudID != "417" || client.fixedCreates != 2 {
		t.Fatalf("fresh lease blocked by rejected attempt: err=%v VMID=%s clones=%d", err, lease.Server.CloudID, client.fixedCreates)
	}
}

func TestProxmoxFixedCloneRejectionWithoutAbsenceProofRetainsAttempt(t *testing.T) {
	for _, scenario := range []string{"VMID exists", "lease VM exists", "inventory unreadable"} {
		t.Run(scenario, func(t *testing.T) {
			backend, client, req := fixedProxmoxFixture(t)
			client.fixedCreateErr = proxmoxCloneRejection()
			switch scenario {
			case "VMID exists":
				client.clusterExistsByID = map[string]bool{"417": true}
			case "lease VM exists":
				client.beforeClone = func(int, map[string]string) error {
					client.servers = []core.Server{{Provider: "proxmox", CloudID: "901", HostID: "pve2", Labels: map[string]string{"lease": req.RequestedLeaseID}}}
					return nil
				}
			case "inventory unreadable":
				client.clusterExistsErr = errors.New("permission denied: VM.Audit")
			}
			_, err := backend.Acquire(context.Background(), req)
			if err == nil || !strings.Contains(err.Error(), "retain rejected Proxmox clone attempt") {
				t.Fatalf("unproven rejection err=%v", err)
			}
			before := readFixedProxmoxClaim(t, req.RequestedLeaseID)
			if before.FixedCreateIntent.State != "prepared" || before.FixedCreateIntent.Attempt["vmid"] != "417" {
				t.Fatalf("unproven rejection settled the attempt: %+v", before)
			}
			requireFixedProxmoxKey(t, req.RequestedLeaseID, true)
			client.fixedCreateErr, client.beforeClone, client.clusterExistsErr = nil, nil, nil
			if _, err := backend.Acquire(context.Background(), req); err == nil || !strings.Contains(err.Error(), "lease_id_conflict") {
				t.Fatalf("unproven rejection replayed: %v", err)
			}
			if client.fixedCreates != 1 || client.deleteCalls != 0 {
				t.Fatalf("clones=%d deletes=%d", client.fixedCreates, client.deleteCalls)
			}
			if after := readFixedProxmoxClaim(t, req.RequestedLeaseID); !reflect.DeepEqual(before, after) {
				t.Fatal("retained attempt changed")
			}
		})
	}
}

func TestProxmoxFixedCloneRejectionThroughHTTPAPI(t *testing.T) {
	for _, tc := range []struct {
		status   int
		terminal bool
	}{
		{status: http.StatusForbidden, terminal: true},
		{status: http.StatusUnauthorized, terminal: true},
		{status: http.StatusInternalServerError},
		{status: http.StatusBadGateway},
	} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			backend, _, req := fixedProxmoxFixture(t)
			var clones, unexpected int
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var data any
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api2/json/access/permissions":
					data = map[string]any{r.URL.Query().Get("path"): map[string]int{"VM.Audit": 1}}
				case r.Method == http.MethodGet && r.URL.Path == "/api2/json/cluster/resources":
					data = []any{}
				case r.Method == http.MethodGet && r.URL.Path == "/api2/json/cluster/nextid":
					data = "102"
				case r.Method == http.MethodPost && r.URL.Path == "/api2/json/nodes/pve1/qemu/9400/clone":
					clones++
					http.Error(w, "Permission check failed (/sdn/zones/localnetwork/vmbr0, SDN.Use)", tc.status)
					return
				default:
					unexpected++
					http.Error(w, "unexpected request", http.StatusInternalServerError)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
			}))
			defer api.Close()
			backend.Cfg.Proxmox.APIURL = api.URL
			newClient = func(cfg core.Config) (proxmoxClient, error) { return core.NewProxmoxClient(cfg) }
			if _, err := backend.Acquire(context.Background(), req); err == nil {
				t.Fatal("expected clone failure")
			}
			claim := readFixedProxmoxClaim(t, req.RequestedLeaseID)
			if got := claim.FixedCreateIntent.State == "released"; got != tc.terminal {
				t.Fatalf("terminal=%t, want %t: %+v", got, tc.terminal, claim)
			}
			if !tc.terminal && claim.FixedCreateIntent.Attempt["vmid"] != "102" {
				t.Fatalf("ambiguous clone failure lost its attempt: %+v", claim)
			}
			if _, err := backend.Acquire(context.Background(), req); err == nil {
				t.Fatal("replay succeeded without a VM")
			}
			if clones != 1 || unexpected != 0 {
				t.Fatalf("clones=%d unexpected=%d", clones, unexpected)
			}
		})
	}
}
