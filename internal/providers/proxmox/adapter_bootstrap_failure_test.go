package proxmox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/openclaw/crabbox/internal/cli"
)

// adapter serve re-executes this test binary for its child commands. The
// children share a file-backed fake Proxmox inventory.
const adapterChildStateEnv = "CRABBOX_PROXMOX_TEST_ADAPTER_STATE"

type adapterChildVM struct {
	VMID       int64             `json:"vmid"`
	Node       string            `json:"node"`
	Name       string            `json:"name"`
	Generation string            `json:"generation"`
	Labels     map[string]string `json:"labels"`
}

type adapterFailedAttempt struct {
	State      string `json:"state"`
	CloudID    string `json:"cloudId"`
	Generation string `json:"generation"`
	VMPresent  bool   `json:"vmPresent"`
	VMReady    bool   `json:"vmReady"`
}

type adapterChildState struct {
	VMs        []adapterChildVM      `json:"vms"`
	Clones     int                   `json:"clones"`
	Bootstraps int                   `json:"bootstraps"`
	Deleted    []string              `json:"deleted"`
	Commands   []string              `json:"commands"`
	Failed     *adapterFailedAttempt `json:"failed,omitempty"`
	// CloneFailure is "rejected" for a definite 403 from the clone endpoint or
	// "uncertain" for a failure after the clone was submitted.
	CloneFailure string   `json:"cloneFailure,omitempty"`
	Errors       []string `json:"errors,omitempty"`
}

func (vm adapterChildVM) server() core.Server {
	server := core.Server{Provider: "proxmox", CloudID: strconv.FormatInt(vm.VMID, 10), ID: vm.VMID, HostID: vm.Node,
		ImmutableID: vm.Generation, Name: vm.Name, Labels: maps.Clone(vm.Labels)}
	return server
}

func adapterChildVMFromServer(server core.Server) adapterChildVM {
	return adapterChildVM{VMID: server.ID, Node: server.HostID, Name: server.Name, Generation: server.ImmutableID, Labels: maps.Clone(server.Labels)}
}

// The fake clone replaces the inventory; keep other leases' VMs beside it.
type adapterChildClient struct{ *fixedProxmoxClient }

func (c *adapterChildClient) CreateServerWithVMID(ctx context.Context, cfg core.Config, publicKey, leaseID, slug string, keep bool, vmid int, labels map[string]string, bind func(core.Server) error) (core.Server, error) {
	others := slices.Clone(c.servers)
	server, err := c.fixedProxmoxClient.CreateServerWithVMID(ctx, cfg, publicKey, leaseID, slug, keep, vmid, labels, bind)
	created := slices.DeleteFunc(c.servers, func(candidate core.Server) bool {
		return slices.ContainsFunc(others, func(other core.Server) bool { return other.CloudID == candidate.CloudID })
	})
	for i := range created {
		// Release reads provider state only; keep best-effort guest cleanup offline.
		created[i].PublicNet.IPv4.IP = ""
	}
	c.servers = append(others, created...)
	return server, err
}

// Like the real client, an empty pool lists as [] rather than null.
func (c *adapterChildClient) ListCrabboxServersCluster(ctx context.Context) ([]core.Server, error) {
	servers, err := c.fixedProxmoxClient.ListCrabboxServersCluster(ctx)
	if servers == nil && err == nil {
		servers = []core.Server{}
	}
	return servers, err
}

func readAdapterChildState(path string) (adapterChildState, error) {
	var state adapterChildState
	data, err := os.ReadFile(path)
	if err != nil {
		return state, err
	}
	return state, json.Unmarshal(data, &state)
}

func writeAdapterChildState(path string, state adapterChildState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	// Replace atomically: an operator command may run beside an adapter child.
	temporary := path + ".tmp-" + strconv.Itoa(os.Getpid())
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

// runAdapterChild is one crabbox child command. Bootstrap always fails, as it
// did when the adapter could not write its temporary files.
func runAdapterChild(path string, args []string) int {
	state, err := readAdapterChildState(path)
	if err != nil || len(args) == 0 {
		fmt.Fprintf(os.Stderr, "adapter test child: args=%v state: %v\n", args, err)
		return 70
	}
	state.Commands = append(state.Commands, args[0])
	client := &adapterChildClient{&fixedProxmoxClient{fakeProxmoxDoctorClient: &fakeProxmoxDoctorClient{}, nextVMID: 417, fixedCreates: state.Clones}}
	for _, vm := range state.VMs {
		client.servers = append(client.servers, vm.server())
	}
	switch state.CloneFailure {
	case "rejected":
		client.fixedCreateErr = &core.ProxmoxError{Method: http.MethodPost, Path: "/nodes/pve1/qemu/9400/clone", StatusCode: http.StatusForbidden,
			Body: "Permission check failed (/sdn/zones/localnetwork/vmbr0, SDN.Use)"}
	case "uncertain":
		client.fixedCreateErr = errors.New("clone task status: http 403: Permission check failed")
	}
	newClient = func(core.Config) (proxmoxClient, error) { return client, nil }
	bootstraps := state.Bootstraps
	waitForSSHReadyFunc = func(context.Context, *core.SSHTarget, io.Writer, string, time.Duration) error {
		state.Bootstraps++
		return errors.New("bootstrap: create temporary file: read-only file system")
	}
	runErr := core.Run(context.Background(), args)
	if args[0] == "warmup" && state.Bootstraps > bootstraps {
		leaseID := ""
		if i := slices.Index(args, "--lease-id"); i >= 0 && i+1 < len(args) {
			leaseID = args[i+1]
		}
		claim, _, err := core.ReadLeaseClaimWithPresence(leaseID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "adapter test child: read failed attempt: %v\n", err)
			return 70
		}
		failed := &adapterFailedAttempt{CloudID: claim.CloudID, Generation: claim.CloudImmutableID}
		if claim.FixedCreateIntent != nil {
			failed.State = claim.FixedCreateIntent.State
		}
		for _, server := range client.servers {
			if server.CloudID == claim.CloudID {
				failed.VMPresent, failed.VMReady = true, server.Labels["state"] == "ready"
			}
		}
		state.Failed = failed
	}
	state.Clones = client.fixedCreates
	state.Deleted = append(state.Deleted, client.deletedIDs...)
	state.VMs = state.VMs[:0]
	for _, server := range client.servers {
		state.VMs = append(state.VMs, adapterChildVMFromServer(server))
	}
	if err := writeAdapterChildState(path, state); err != nil {
		fmt.Fprintf(os.Stderr, "adapter test child: write state: %v\n", err)
		return 70
	}
	if runErr != nil {
		state.Errors = append(state.Errors, args[0]+": "+runErr.Error())
		_ = writeAdapterChildState(path, state)
		fmt.Fprintln(os.Stderr, runErr)
		var exit core.ExitError
		if core.AsExitError(runErr, &exit) {
			return exit.Code
		}
		return 1
	}
	return 0
}

type proxmoxAdapterHarness struct {
	t         *testing.T
	inventory string
	client    *http.Client
	logs      bytes.Buffer
	served    chan error
	cancel    context.CancelFunc
	stopped   bool
}

const proxmoxAdapterTestToken = "adapter-test-token"

// startProxmoxAdapter runs adapter serve in this process. Its crabbox children
// are this test binary, sharing the fake inventory file.
func startProxmoxAdapter(t *testing.T, dir, inventory string, extraArgs ...string) *proxmoxAdapterHarness {
	t.Helper()
	t.Setenv(adapterChildStateEnv, inventory)
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte(proxmoxAdapterTestToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	socketDir, err := os.MkdirTemp("", "cbx-adapter-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "adapter.sock")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	h := &proxmoxAdapterHarness{t: t, inventory: inventory, served: make(chan error, 1), cancel: cancel}
	args := append([]string{"adapter", "serve",
		"--provider", "proxmox", "--token-file", tokenFile, "--state-file", filepath.Join(dir, "adapter-state.json"),
		"--listen", "127.0.0.1:0", "--unix-socket", socket, "--crabbox-binary", binary, "--work-dir", dir}, extraArgs...)
	go func() { h.served <- (core.App{Stdout: io.Discard, Stderr: &h.logs}).Run(ctx, args) }()
	t.Cleanup(func() { _ = h.stop() })
	h.client = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
	deadline := time.Now().Add(time.Minute)
	for {
		if code, _ := h.call(http.MethodGet, "/v1/workspaces/probe", nil); code != 0 {
			return h
		}
		select {
		case err := <-h.served:
			h.stopped = true
			t.Fatalf("adapter exited: %v\n%s", err, h.logs.String())
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("adapter did not start")
		}
	}
}

func (h *proxmoxAdapterHarness) call(method, path string, body any) (int, map[string]any) {
	h.t.Helper()
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		payload = bytes.NewReader(data)
	}
	request, err := http.NewRequest(method, "http://adapter"+path, payload)
	if err != nil {
		h.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+proxmoxAdapterTestToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := h.client.Do(request)
	if err != nil {
		return 0, nil
	}
	defer response.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(response.Body).Decode(&decoded)
	return response.StatusCode, decoded
}

// wait polls one workspace until done accepts it with the fake inventory.
func (h *proxmoxAdapterHarness) wait(id string, timeout time.Duration, done func(map[string]any, adapterChildState) bool) (map[string]any, adapterChildState) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, workspace := h.call(http.MethodGet, "/v1/workspaces/"+id, nil)
		state, err := readAdapterChildState(h.inventory)
		if err == nil && done(workspace, state) {
			return workspace, state
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("workspace %s did not reach the expected state: %v; children=%v deleted=%v errors=%q", id, workspace, state.Commands, state.Deleted, state.Errors[:min(len(state.Errors), 3)])
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (h *proxmoxAdapterHarness) stop() error {
	if h.stopped {
		return nil
	}
	h.stopped = true
	h.cancel()
	return <-h.served
}

func adapterWorkspaceSettled(workspace map[string]any, _ adapterChildState) bool {
	status, _ := workspace["status"].(string)
	return status != "" && status != "provisioning" && status != "stopping"
}

// A fixed clone succeeds and its generation is bound, then bootstrap fails.
// Ordinary adapter reconciliation must delete exactly that VM and finish the
// workspace well inside the 60-minute create timeout.
func TestProxmoxAdapterDeletesGenerationBoundVMAfterBootstrapFailure(t *testing.T) {
	t.Run("unregistered", func(t *testing.T) { testProxmoxAdapterBootstrapFailure(t, false) })
	t.Run("registered", func(t *testing.T) { testProxmoxAdapterBootstrapFailure(t, true) })
}

func testProxmoxAdapterBootstrapFailure(t *testing.T, registered bool) {
	cfg := controllerTestConfig()
	setControllerTestEnv(t, cfg)
	var coordinatorMu sync.Mutex
	var coordinatorCalls []string
	if registered {
		// Registration follows readiness, so this coordinator never learns the
		// lease and answers every request for it as the real one does: 404.
		coordinator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			coordinatorMu.Lock()
			coordinatorCalls = append(coordinatorCalls, r.Method+" "+r.URL.Path+" "+string(body))
			coordinatorMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not_found","message":"lease not found"}`))
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
	other := adapterChildVM{VMID: 102, Node: "pve1", Name: "crabbox-other-box", Generation: replacementGeneration, Labels: map[string]string{
		"crabbox": "true", "provider": "proxmox", "lease": "cbx_aaaaaaaaaaaa", "slug": "other-box", "state": "ready",
	}}
	if err := writeAdapterChildState(inventory, adapterChildState{VMs: []adapterChildVM{other}}); err != nil {
		t.Fatal(err)
	}
	// The default --create-timeout is 60 minutes.
	adapter := startProxmoxAdapter(t, dir, inventory)
	if code, body := adapter.call(http.MethodPost, "/v1/workspaces", map[string]any{"id": "bootstrap-failure"}); code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%v", code, body)
	}
	workspace, _ := adapter.wait("bootstrap-failure", 2*time.Minute, adapterWorkspaceSettled)
	if err := adapter.stop(); err != nil {
		t.Fatalf("adapter serve: %v", err)
	}
	if workspace["status"] != "failed" || workspace["message"] != "workspace provisioning failed before provider identity acknowledgment" {
		t.Fatalf("terminal workspace=%v", workspace)
	}
	state, err := readAdapterChildState(inventory)
	if err != nil {
		t.Fatal(err)
	}
	// The starting point: a prepared claim bound to the clone's generation and a
	// live VM that never became ready.
	if failed := state.Failed; failed == nil || failed.State != "prepared" || failed.CloudID != "417" ||
		failed.Generation != fixedTestGeneration || !failed.VMPresent || failed.VMReady {
		t.Fatalf("bootstrap failure did not leave a generation-bound attempt: %+v", state.Failed)
	}
	leaseID, _ := workspace["leaseId"].(string)
	if workspace["providerResourceId"] != "417" || !core.IsCanonicalLeaseID(leaseID) {
		t.Fatalf("workspace identity=%v", workspace)
	}
	t.Logf("adapter children: %s", strings.Join(state.Commands, " "))
	warmups := 0
	for _, command := range state.Commands {
		if command == "warmup" {
			warmups++
		}
	}
	// One create and one identity replay; neither the replay nor cleanup clones
	// or bootstraps again.
	if warmups != 2 || state.Clones != 1 || state.Bootstraps != 1 {
		t.Fatalf("warmups=%d clones=%d bootstraps=%d children=%v", warmups, state.Clones, state.Bootstraps, state.Commands)
	}
	if !reflect.DeepEqual(state.Deleted, []string{"417"}) || !reflect.DeepEqual(state.VMs, []adapterChildVM{other}) {
		t.Fatalf("deleted=%v remaining=%+v", state.Deleted, state.VMs)
	}
	receipt := readFixedProxmoxClaim(t, leaseID)
	if receipt.FixedCreateIntent.State != "released" || receipt.CloudID != "417" || receipt.CloudImmutableID != fixedTestGeneration {
		t.Fatalf("receipt=%+v", receipt)
	}
	if strings.Contains(adapter.logs.String(), controllerTestSecret) {
		t.Fatal("adapter log exposed the token secret")
	}
	coordinatorMu.Lock()
	defer coordinatorMu.Unlock()
	completed := false
	for _, call := range coordinatorCalls {
		if !strings.HasPrefix(call, "POST /v1/leases/"+leaseID+"/release ") {
			t.Fatalf("unexpected coordinator request %q", call)
		}
		completed = completed || strings.Contains(call, `"runtimeAdapterLegacyDeleteCompletion":{"adapterID":"proxmox-lab","status":"absent","workspaceID":"bootstrap-failure"}`)
	}
	if registered != completed {
		t.Fatalf("registered=%t delete completion=%t calls=%v", registered, completed, coordinatorCalls)
	}
}

// Replay reports an unready VM only when the claim's bound generation proves it
// is this attempt's clone. It never resumes bootstrap or changes the claim.
func TestProxmoxFixedReplayReportsOnlyGenerationBoundUnreadyVM(t *testing.T) {
	for _, scenario := range []string{"bound generation", "unbound generation", "replaced generation"} {
		t.Run(scenario, func(t *testing.T) {
			backend, client, req := fixedProxmoxFixture(t)
			bootstrapErr := errors.New("bootstrap failed")
			waitForSSHReadyFunc = func(context.Context, *core.SSHTarget, io.Writer, string, time.Duration) error { return bootstrapErr }
			switch scenario {
			case "unbound generation":
				// The clone may exist, but its identity read failed before binding.
				client.fixedCreateErr = errors.New("read cloned VM config: http 403")
				if _, err := backend.Acquire(t.Context(), req); err == nil || !strings.Contains(err.Error(), "uncertain") {
					t.Fatalf("uncertain clone: %v", err)
				}
				claim := readFixedProxmoxClaim(t, req.RequestedLeaseID)
				if claim.CloudImmutableID != "" {
					t.Fatalf("uncertain clone bound a generation: %+v", claim)
				}
				labels := maps.Clone(claim.Labels)
				maps.Copy(labels, map[string]string{"lease": req.RequestedLeaseID, "slug": claim.Slug, "provider": "proxmox", "crabbox": "true"})
				client.servers = []core.Server{{Provider: "proxmox", CloudID: "417", ID: 417, HostID: "pve1", ImmutableID: fixedTestGeneration, Name: "crabbox-" + claim.Slug, Labels: labels}}
			default:
				if _, err := backend.Acquire(t.Context(), req); !errors.Is(err, bootstrapErr) {
					t.Fatalf("bootstrap failure: %v", err)
				}
				if scenario == "replaced generation" {
					client.servers[0].ImmutableID = replacementGeneration
				}
			}
			before := readFixedProxmoxClaim(t, req.RequestedLeaseID)
			var reported []core.LeaseTarget
			req.OnAcquired = func(lease core.LeaseTarget) error {
				reported = append(reported, lease)
				return nil
			}
			waitForSSHReadyFunc = func(context.Context, *core.SSHTarget, io.Writer, string, time.Duration) error {
				t.Fatal("replay resumed bootstrap")
				return nil
			}
			if _, err := backend.Acquire(t.Context(), req); err == nil || !strings.Contains(err.Error(), "lease_id_conflict") {
				t.Fatalf("replay adopted the attempt: %v", err)
			}
			if scenario == "bound generation" {
				if len(reported) != 1 || reported[0].LeaseID != req.RequestedLeaseID || reported[0].Server.CloudID != "417" ||
					reported[0].Server.ImmutableID != fixedTestGeneration {
					t.Fatalf("reported=%+v", reported)
				}
			} else if len(reported) != 0 {
				t.Fatalf("unproven VM identity reported: %+v", reported)
			}
			if client.fixedCreates != 1 || client.deleteCalls != 0 || !reflect.DeepEqual(before, readFixedProxmoxClaim(t, req.RequestedLeaseID)) {
				t.Fatalf("replay changed state: clones=%d deletes=%d", client.fixedCreates, client.deleteCalls)
			}
		})
	}
}
