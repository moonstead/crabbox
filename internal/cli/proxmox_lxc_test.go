package cli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const proxmoxLXCTestTemplate = "local:vztmpl/stead-headless-lxc-2404-20260926-0123456789ab.tar.zst"

func proxmoxLXCTestConfig(apiURL string) Config {
	cfg := baseConfig()
	cfg.Provider = "proxmox"
	cfg.SSHUser = "crabbox"
	cfg.WorkRoot = "/work/crabbox"
	cfg.Proxmox.APIURL = apiURL
	cfg.Proxmox.TokenID = "runner@pve!crabbox"
	cfg.Proxmox.TokenSecret = "secret"
	cfg.Proxmox.Node = "pve1"
	cfg.Proxmox.Guest = ProxmoxGuestLXC
	cfg.Proxmox.LXCTemplate = proxmoxLXCTestTemplate
	cfg.Proxmox.LXCCores = 2
	cfg.Proxmox.LXCMemoryMiB = 4096
	cfg.Proxmox.LXCSwapMiB = 512
	cfg.Proxmox.LXCDiskGiB = 16
	cfg.Proxmox.Storage = "local-lvm"
	cfg.Proxmox.Pool = "stead-lxc"
	cfg.Proxmox.Bridge = "vmbr0"
	cfg.ServerType = "lxc"
	return cfg
}

// fakeProxmoxLXCAPI keeps the created container's configuration and answers
// the container lifecycle endpoints from it.
type fakeProxmoxLXCAPI struct {
	t        *testing.T
	mu       sync.Mutex
	create   url.Values
	config   map[string]any
	running  bool
	deleted  bool
	events   []string
	deleteQS url.Values
	extra    map[string]any
}

func (f *fakeProxmoxLXCAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	reply := func(data any) { _ = json.NewEncoder(w).Encode(map[string]any{"data": data}) }
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api2/json/nodes/pve1/lxc":
		f.create = readForm(f.t, r)
		f.events = append(f.events, "create")
		f.config = map[string]any{
			"arch": "amd64", "ostype": "ubuntu", "digest": "d",
			"hostname": f.create.Get("hostname"), "description": f.create.Get("description"),
			"tags": f.create.Get("tags"), "unprivileged": 1,
			"cores": 2, "memory": 4096, "swap": 512, "onboot": 0,
			"rootfs": "local-lvm:vm-1000-disk-0,size=16G",
			"net0":   "name=eth0,bridge=vmbr0,hwaddr=BC:24:11:00:00:01,ip=dhcp,type=veth",
		}
		for key, value := range f.extra {
			f.config[key] = value
		}
		reply("UPID:pve1:vzcreate")
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api2/json/nodes/pve1/tasks/"):
		f.events = append(f.events, "wait")
		reply(map[string]any{"status": "stopped", "exitstatus": "OK"})
	case r.Method == http.MethodGet && r.URL.Path == "/api2/json/nodes/pve1/lxc/1000/status/current":
		if f.config == nil || f.deleted {
			w.WriteHeader(http.StatusNotFound)
			reply(nil)
			return
		}
		status := "stopped"
		if f.running {
			status = "running"
		}
		reply(map[string]any{"vmid": 1000, "name": f.config["hostname"], "status": status})
	case r.Method == http.MethodGet && r.URL.Path == "/api2/json/nodes/pve1/lxc/1000/config":
		f.events = append(f.events, "config")
		reply(f.config)
	case r.Method == http.MethodPost && r.URL.Path == "/api2/json/nodes/pve1/lxc/1000/status/start":
		f.events = append(f.events, "start")
		f.running = true
		reply("UPID:pve1:vzstart")
	case r.Method == http.MethodPost && r.URL.Path == "/api2/json/nodes/pve1/lxc/1000/status/stop":
		f.events = append(f.events, "stop")
		f.running = false
		reply("UPID:pve1:vzstop")
	case r.Method == http.MethodDelete && r.URL.Path == "/api2/json/nodes/pve1/lxc/1000":
		f.events = append(f.events, "delete")
		f.deleteQS = r.URL.Query()
		f.deleted = true
		reply("UPID:pve1:vzdestroy")
	case r.Method == http.MethodGet && r.URL.Path == "/api2/json/nodes/pve1/lxc/1000/interfaces":
		if !f.running {
			reply(nil)
			return
		}
		reply([]any{
			map[string]any{"name": "lo", "ip-addresses": []any{map[string]any{"ip-address-type": "inet", "ip-address": "127.0.0.1"}}},
			map[string]any{"name": "eth0", "ip-addresses": []any{
				map[string]any{"ip-address-type": "inet6", "ip-address": "fe80::1"},
				map[string]any{"ip-address-type": "inet", "ip-address": "192.0.2.61"},
			}},
		})
	default:
		f.t.Errorf("unexpected %s %s", r.Method, r.URL.String())
		w.WriteHeader(http.StatusTeapot)
	}
}

func installProxmoxLXCSSHFake(t *testing.T) *[]SSHTarget {
	t.Helper()
	var targets []SSHTarget
	probe, input := proxmoxRunSSHQuietWithOptions, proxmoxRunSSHInput
	proxmoxRunSSHQuietWithOptions = func(context.Context, SSHTarget, string, string, string) error { return nil }
	proxmoxRunSSHInput = func(_ context.Context, target SSHTarget, command string, stdin io.Reader, _, _ io.Writer) error {
		script, err := io.ReadAll(stdin)
		if err != nil {
			return err
		}
		if command != "/bin/bash -s" || !strings.Contains(string(script), "PermitRootLogin no") {
			t.Errorf("container bootstrap command=%q", command)
		}
		targets = append(targets, target)
		return nil
	}
	t.Cleanup(func() { proxmoxRunSSHQuietWithOptions, proxmoxRunSSHInput = probe, input })
	return &targets
}

func TestProxmoxLXCCreateServerFlow(t *testing.T) {
	api := &fakeProxmoxLXCAPI{t: t}
	server := httptest.NewServer(api)
	defer server.Close()
	targets := installProxmoxLXCSSHFake(t)
	cfg := proxmoxLXCTestConfig(server.URL)
	client, err := NewProxmoxClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := client.CreateServerWithVMID(ctx, cfg, "ssh-ed25519 AAAAfixture lease", "cbx_123456abcdef", "blue-crab", false, 1000, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	form := api.create
	for key, want := range map[string]string{
		"vmid": "1000", "ostemplate": proxmoxLXCTestTemplate, "unprivileged": "1",
		"ssh-public-keys": "ssh-ed25519 AAAAfixture lease", "rootfs": "local-lvm:16",
		"cores": "2", "memory": "4096", "swap": "512", "pool": "stead-lxc", "tags": "crabbox",
		"net0": "name=eth0,bridge=vmbr0,ip=dhcp,type=veth", "onboot": "0", "start": "0",
		"hostname": LeaseProviderName("cbx_123456abcdef", "blue-crab"),
	} {
		if form.Get(key) != want {
			t.Errorf("create %s=%q, want %q", key, form.Get(key), want)
		}
	}
	for key := range form {
		switch key {
		case "features", "mp0", "dev0", "hookscript", "lxc", "password", "restore", "force":
			t.Errorf("create requested forbidden %s", key)
		}
	}
	labels := proxmoxDescriptionLabels(form.Get("description"))
	generation := labels[ProxmoxLXCGenerationLabel]
	if proxmoxLXCGenerationID(generation) == "" || labels["guest"] != "lxc" || labels["template_id"] != proxmoxLXCTestTemplate || labels["lease"] != "cbx_123456abcdef" {
		t.Fatalf("labels=%v", labels)
	}
	if got.CloudID != "1000" || got.ImmutableID != generation || got.PublicNet.IPv4.IP != "192.0.2.61" || got.Provider != "proxmox" {
		t.Fatalf("server=%+v", got)
	}
	if len(*targets) != 1 || (*targets)[0].User != "root" || (*targets)[0].Host != "192.0.2.61" {
		t.Fatalf("bootstrap targets=%+v", *targets)
	}
	if !reflect.DeepEqual(api.events[:5], []string{"create", "wait", "config", "config", "start"}) {
		t.Fatalf("events=%v", api.events)
	}
}

func TestProxmoxLXCCreateRefusesUnexpectedConfiguration(t *testing.T) {
	for name, extra := range map[string]map[string]any{
		"nesting":     {"features": "nesting=1"},
		"bind mount":  {"mp0": "/srv/host,mp=/mnt/host"},
		"device":      {"dev0": "/dev/kvm"},
		"raw lxc":     {"lxc": []any{[]any{"lxc.apparmor.profile", "unconfined"}}},
		"privileged":  {"unprivileged": 0},
		"hookscript":  {"hookscript": "local:snippets/hook.pl"},
		"more memory": {"memory": 8192},
	} {
		t.Run(name, func(t *testing.T) {
			api := &fakeProxmoxLXCAPI{t: t, extra: extra}
			server := httptest.NewServer(api)
			defer server.Close()
			installProxmoxLXCSSHFake(t)
			cfg := proxmoxLXCTestConfig(server.URL)
			client, err := NewProxmoxClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.CreateServerWithVMID(context.Background(), cfg, "ssh-ed25519 AAAAfixture", "cbx_123456abcdef", "blue-crab", false, 1000, nil, nil)
			if err == nil || !strings.Contains(err.Error(), "configuration refused") {
				t.Fatalf("err=%v", err)
			}
			if api.running || !api.deleted || api.deleteQS.Get("purge") != "1" || api.deleteQS.Get("destroy-unreferenced-disks") != "1" {
				t.Fatalf("refused container not removed: events=%v query=%v", api.events, api.deleteQS)
			}
		})
	}
}

func TestProxmoxLXCFixedCreateBindsBeforeAuditAndKeepsCustody(t *testing.T) {
	api := &fakeProxmoxLXCAPI{t: t, extra: map[string]any{"features": "nesting=1"}}
	server := httptest.NewServer(api)
	defer server.Close()
	installProxmoxLXCSSHFake(t)
	cfg := proxmoxLXCTestConfig(server.URL)
	client, err := NewProxmoxClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var bound Server
	labels := map[string]string{"fixed_intent_sha256": strings.Repeat("a", 64)}
	_, err = client.CreateServerWithVMID(context.Background(), cfg, "ssh-ed25519 AAAAfixture", "cbx_123456abcdef", "blue-crab", false, 1000, labels, func(created Server) error {
		bound = created
		return nil
	})
	if err == nil || bound.ImmutableID == "" || bound.CloudID != "1000" {
		t.Fatalf("err=%v bound=%+v", err, bound)
	}
	if api.deleted || api.running {
		t.Fatalf("fixed refused container left custody: events=%v", api.events)
	}
}

func TestProxmoxLXCCreateRequiresFixedBinding(t *testing.T) {
	cfg := proxmoxLXCTestConfig("https://pve.invalid")
	client, err := NewProxmoxClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CreateServerWithVMID(context.Background(), cfg, "ssh-ed25519 AAAAfixture", "cbx_123456abcdef", "blue-crab", false, 1000, map[string]string{"fixed_intent_sha256": "x"}, nil)
	if err == nil || !strings.Contains(err.Error(), "durable generation binding") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateProxmoxGuestConfig(t *testing.T) {
	if err := ValidateProxmoxGuestConfig(proxmoxLXCTestConfig("https://pve.invalid")); err != nil {
		t.Fatal(err)
	}
	qemu := baseConfig()
	qemu.Proxmox.TemplateID = 9420
	if err := ValidateProxmoxGuestConfig(qemu); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Config){
		"unknown guest":        func(cfg *Config) { cfg.Proxmox.Guest = "vm" },
		"qemu with lxc fields": func(cfg *Config) { cfg.Proxmox.Guest = "qemu" },
		"template path":        func(cfg *Config) { cfg.Proxmox.LXCTemplate = "local:iso/ubuntu.iso" },
		"template traversal":   func(cfg *Config) { cfg.Proxmox.LXCTemplate = "local:vztmpl/../x.tar.zst" },
		"qemu template":        func(cfg *Config) { cfg.Proxmox.TemplateID = 9420 },
		"desktop template":     func(cfg *Config) { cfg.Proxmox.TemplateDesktop = true },
		"browser template":     func(cfg *Config) { cfg.Proxmox.TemplateBrowser = true },
		"no cores":             func(cfg *Config) { cfg.Proxmox.LXCCores = 0 },
		"too many cores":       func(cfg *Config) { cfg.Proxmox.LXCCores = 65 },
		"little memory":        func(cfg *Config) { cfg.Proxmox.LXCMemoryMiB = 128 },
		"negative swap":        func(cfg *Config) { cfg.Proxmox.LXCSwapMiB = -1 },
		"small disk":           func(cfg *Config) { cfg.Proxmox.LXCDiskGiB = 2 },
		"no storage":           func(cfg *Config) { cfg.Proxmox.Storage = "" },
		"no bridge":            func(cfg *Config) { cfg.Proxmox.Bridge = "" },
		"root user":            func(cfg *Config) { cfg.SSHUser = "root" },
		"desktop lease":        func(cfg *Config) { cfg.Desktop = true },
		"browser lease":        func(cfg *Config) { cfg.Browser = true },
		"code lease":           func(cfg *Config) { cfg.Code = true },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := proxmoxLXCTestConfig("https://pve.invalid")
			mutate(&cfg)
			if err := ValidateProxmoxGuestConfig(cfg); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestProxmoxLXCGuestIPv4(t *testing.T) {
	for name, tc := range map[string]struct {
		interfaces any
		want       string
	}{
		"typed address": {[]any{map[string]any{"name": "eth0", "ip-addresses": []any{map[string]any{"ip-address-type": "inet", "ip-address": "192.0.2.9"}}}}, "192.0.2.9"},
		"legacy inet":   {[]any{map[string]any{"name": "eth0", "inet": "192.0.2.10/24"}}, "192.0.2.10"},
		"link local":    {[]any{map[string]any{"name": "eth0", "inet": "169.254.1.2/16"}}, ""},
		"other iface":   {[]any{map[string]any{"name": "docker0", "inet": "172.17.0.1/16"}}, ""},
		"loopback only": {[]any{map[string]any{"name": "lo", "inet": "127.0.0.1/8"}}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"data": tc.interfaces})
			}))
			defer server.Close()
			client := &ProxmoxClient{BaseURL: server.URL, Node: "pve1", Guest: ProxmoxGuestLXC, Client: server.Client()}
			got, err := client.lxcGuestIPv4(context.Background(), 1000)
			if got != tc.want || (tc.want == "") != (err != nil) {
				t.Fatalf("ip=%q err=%v", got, err)
			}
		})
	}
}

func TestProxmoxLXCBootstrapScriptHandsOffRootAccess(t *testing.T) {
	cfg := proxmoxLXCTestConfig("https://pve.invalid")
	cfg.WorkRoot = "/work/crab box"
	script := proxmoxLXCBootstrapScript(cfg)
	for _, want := range []string{
		"user='crabbox'", "work_root='/work/crab box'", "PermitRootLogin no",
		"rm -f /root/.ssh/authorized_keys", `install -m 0600 -o "$user" -g "$user" /root/.ssh/authorized_keys`,
		"sshd -t", "runuser -u \"$user\" -- /usr/local/bin/crabbox-ready", "touch /var/lib/crabbox/bootstrapped",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("bootstrap lacks %q", want)
		}
	}
	for _, forbidden := range []string{"apt-get", "http://", "https://", "wget", "NOPASSWD", "nesting"} {
		if strings.Contains(script, forbidden) {
			t.Errorf("bootstrap contains %q", forbidden)
		}
	}
	if strings.Index(script, "rm -f /root/.ssh/authorized_keys") < strings.Index(script, "authorized_keys \"$home/.ssh/authorized_keys\"") {
		t.Fatal("root key removed before the lease user received it")
	}
}

func TestProxmoxLXCLifecycleUsesContainerEndpoints(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
		reply := func(data any) { _ = json.NewEncoder(w).Encode(map[string]any{"data": data}) }
		switch {
		case r.URL.Path == "/api2/json/access/permissions":
			reply(map[string]any{r.URL.Query().Get("path"): map[string]int{"VM.Audit": 1, "Sys.Audit": 1}})
		case r.URL.Path == "/api2/json/cluster/resources":
			reply([]any{
				map[string]any{"vmid": 1000, "name": "crabbox-blue-crab-12345678", "node": "pve1", "type": "lxc"},
				map[string]any{"vmid": 1001, "name": "crabbox-green-crab-12345678", "node": "pve1", "type": "qemu"},
			})
		case r.URL.Path == "/api2/json/nodes/pve1/lxc/1000/status/current":
			reply(map[string]any{"vmid": 1000, "name": "crabbox-blue-crab-12345678", "status": "running"})
		case r.URL.Path == "/api2/json/nodes/pve1/lxc/1000/config":
			reply(map[string]any{"hostname": "crabbox-blue-crab-12345678", "description": "crabbox labels\ncrabbox=true\nprovider=proxmox\nlease=cbx_123456abcdef\nlxc_generation=" + strings.Repeat("ab", 16) + "\n"})
		case r.URL.Path == "/api2/json/nodes/pve1/lxc/1000/interfaces":
			reply([]any{map[string]any{"name": "eth0", "inet": "192.0.2.61/24"}})
		case r.URL.Path == "/api2/json/nodes/pve1/tasks":
			reply([]any{})
		case strings.HasPrefix(r.URL.Path, "/api2/json/nodes/pve1/tasks/"):
			reply(map[string]any{"status": "stopped", "exitstatus": "OK"})
		case r.URL.Path == "/api2/json/nodes/pve1/lxc/1000/status/stop" || r.URL.Path == "/api2/json/nodes/pve1/lxc/1000":
			reply("UPID:pve1:task")
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	client, err := NewProxmoxClient(proxmoxLXCTestConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	servers, err := client.ListCrabboxServersCluster(context.Background())
	if err != nil || len(servers) != 1 || servers[0].CloudID != "1000" || servers[0].ImmutableID != strings.Repeat("ab", 16) || servers[0].PublicNet.IPv4.IP != "192.0.2.61" {
		t.Fatalf("servers=%+v err=%v", servers, err)
	}
	if err := client.VerifyNoActiveCloneTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteServer(context.Background(), "1000"); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(paths, "\n")
	for _, want := range []string{
		"GET /api2/json/nodes/pve1/tasks?source=active&typefilter=vzcreate&limit=1",
		"POST /api2/json/nodes/pve1/lxc/1000/status/stop?",
		"DELETE /api2/json/nodes/pve1/lxc/1000?destroy-unreferenced-disks=1&purge=1",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "/qemu/") {
		t.Fatalf("container lifecycle touched QEMU endpoints:\n%s", joined)
	}
}

func TestProxmoxLXCDoctorReadiness(t *testing.T) {
	for _, tc := range []struct {
		name         string
		poolPrivs    map[string]int
		volumes      []any
		wantTemplate string
		wantPerms    string
	}{
		{
			name:         "ready",
			poolPrivs:    map[string]int{"VM.Allocate": 1, "VM.Audit": 1, "VM.Config.CPU": 1, "VM.Config.Disk": 1, "VM.Config.Memory": 1, "VM.Config.Network": 1, "VM.Config.Options": 1, "VM.PowerMgmt": 1},
			volumes:      []any{map[string]any{"volid": proxmoxLXCTestTemplate, "content": "vztmpl"}},
			wantTemplate: "ok", wantPerms: "ok",
		},
		{
			name:         "missing resource privileges and template",
			poolPrivs:    map[string]int{"VM.Allocate": 1, "VM.Audit": 1, "VM.Config.Disk": 1, "VM.Config.Network": 1, "VM.Config.Options": 1, "VM.PowerMgmt": 1},
			volumes:      []any{map[string]any{"volid": "local:vztmpl/other.tar.zst", "content": "vztmpl"}},
			wantTemplate: "failed", wantPerms: "failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reply := func(data any) { _ = json.NewEncoder(w).Encode(map[string]any{"data": data}) }
				switch {
				case r.URL.Path == "/api2/json/version":
					reply(map[string]any{"version": "9.2"})
				case r.URL.Path == "/api2/json/nodes/pve1/status":
					reply(map[string]any{"uptime": 1})
				case r.URL.Path == "/api2/json/nodes/pve1/storage":
					reply([]any{
						map[string]any{"storage": "local-lvm", "active": 1, "enabled": 1, "content": "images,rootdir"},
						map[string]any{"storage": "local", "active": 1, "enabled": 1, "content": "iso,vztmpl,backup"},
					})
				case r.URL.Path == "/api2/json/nodes/pve1/network":
					reply([]any{map[string]any{"iface": "vmbr0", "type": "bridge", "active": 1}})
				case r.URL.Path == "/api2/json/nodes/pve1/storage/local/content":
					reply(tc.volumes)
				case r.URL.Path == "/api2/json/access/permissions":
					path := r.URL.Query().Get("path")
					switch path {
					case "/pool/stead-lxc":
						reply(map[string]any{path: tc.poolPrivs})
					case "/storage/local-lvm":
						reply(map[string]any{path: map[string]int{"Datastore.AllocateSpace": 1, "Datastore.Audit": 1}})
					case "/storage/local", "/vms":
						reply(map[string]any{path: map[string]int{"Datastore.Audit": 1, "VM.Audit": 1}})
					default:
						t.Errorf("permission path %q", path)
					}
				case r.URL.Path == "/api2/json/cluster/nextid":
					reply(1000)
				case r.URL.Path == "/api2/json/pools/stead-lxc":
					reply(map[string]any{"members": []any{}})
				case r.URL.Path == "/api2/json/cluster/resources":
					reply([]any{})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.String())
				}
			}))
			defer server.Close()
			cfg := proxmoxLXCTestConfig(server.URL)
			client, err := NewProxmoxClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			checks, err := client.DoctorReadiness(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			statuses := map[string]string{}
			for _, check := range checks {
				statuses[check.Check] = check.Status
			}
			if statuses["storage"] != "ok" || statuses["bridge"] != "ok" || statuses["template"] != tc.wantTemplate || statuses["lxc_permissions"] != tc.wantPerms {
				t.Fatalf("checks=%+v", checks)
			}
		})
	}
}

func TestProxmoxLXCSetLabelsUsesContainerConfigPut(t *testing.T) {
	var method, path string
	var form url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, form = r.Method, r.URL.Path, readForm(t, r)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": nil})
	}))
	defer server.Close()
	client, err := NewProxmoxClient(proxmoxLXCTestConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SetLabelsOnNode(context.Background(), "pve1", "1000", map[string]string{"state": "ready", ProxmoxLXCGenerationLabel: strings.Repeat("ab", 16)}); err != nil {
		t.Fatal(err)
	}
	labels := proxmoxDescriptionLabels(form.Get("description"))
	if method != http.MethodPut || path != "/api2/json/nodes/pve1/lxc/1000/config" || labels["state"] != "ready" || labels[ProxmoxLXCGenerationLabel] == "" {
		t.Fatalf("method=%s path=%s labels=%v", method, path, labels)
	}
}
