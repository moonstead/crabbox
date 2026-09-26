package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Proxmox LXC leases are headless, unprivileged containers created from one
// immutable vztmpl archive. Proxmox itself forbids privileged containers,
// feature flags other than nesting, bind mounts, device passthrough and
// hookscripts to every principal except root@pam, and Crabbox never asks for
// nesting. Each created configuration is still audited against the exact
// allowed shape before the container starts.
//
// Proxmox installs a create-time SSH key only for root. The container's first
// SSH session, as root, moves that per-lease key to the lease user and
// disables root login; every later session uses the lease user.
const (
	ProxmoxGuestQEMU = "qemu"
	ProxmoxGuestLXC  = "lxc"

	// ProxmoxLXCGenerationLabel carries a random identity chosen for each
	// created container. LXC has no vmgenid, so this label binds a fixed
	// lease to exactly the container it created.
	ProxmoxLXCGenerationLabel = "lxc_generation"

	proxmoxLXCBootstrapUser = "root"
	proxmoxLeaseTag         = "crabbox"
	proxmoxLXCMaxCores      = 64
	proxmoxLXCMinMemoryMiB  = 512
	proxmoxLXCMaxMemoryMiB  = 262144
	proxmoxLXCMaxSwapMiB    = 65536
	proxmoxLXCMinDiskGiB    = 4
	proxmoxLXCMaxDiskGiB    = 1024
)

var (
	proxmoxLXCTemplatePattern   = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}:vztmpl/[A-Za-z0-9][A-Za-z0-9._-]{0,199}\.tar\.(zst|xz|gz)$`)
	proxmoxLXCGenerationPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
	proxmoxStorageIDPattern     = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)
	proxmoxBridgePattern        = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,31}$`)
)

// ProxmoxGuest returns the normalized configured lease type.
func ProxmoxGuest(cfg Config) string {
	guest := strings.ToLower(strings.TrimSpace(cfg.Proxmox.Guest))
	if guest == "" {
		return ProxmoxGuestQEMU
	}
	return guest
}

// ProxmoxTemplateLabel is the template identity recorded on every lease: the
// QEMU template VMID, or the LXC vztmpl volume.
func ProxmoxTemplateLabel(cfg Config) string {
	if ProxmoxGuest(cfg) == ProxmoxGuestLXC {
		return strings.TrimSpace(cfg.Proxmox.LXCTemplate)
	}
	return strconv.Itoa(cfg.Proxmox.TemplateID)
}

// ValidateProxmoxGuestConfig rejects unknown lease types and any LXC setting
// that is missing, out of range or mixed with QEMU template settings.
func ValidateProxmoxGuestConfig(cfg Config) error {
	p := cfg.Proxmox
	switch ProxmoxGuest(cfg) {
	case ProxmoxGuestQEMU:
		if strings.TrimSpace(p.LXCTemplate) != "" || p.LXCCores != 0 || p.LXCMemoryMiB != 0 || p.LXCSwapMiB != 0 || p.LXCDiskGiB != 0 {
			return Exit(2, "proxmox lxcTemplate and lxc resource settings require proxmox.guest=lxc")
		}
		return nil
	case ProxmoxGuestLXC:
	default:
		return Exit(2, "proxmox guest must be qemu or lxc")
	}
	switch {
	case !proxmoxLXCTemplatePattern.MatchString(strings.TrimSpace(p.LXCTemplate)):
		return Exit(3, "proxmox guest=lxc requires lxcTemplate as a vztmpl volume, for example local:vztmpl/<name>.tar.zst")
	case p.TemplateID != 0 || p.TemplateDesktop || p.TemplateBrowser:
		return Exit(2, "proxmox guest=lxc does not use templateId, templateDesktop or templateBrowser")
	case p.LXCCores < 1 || p.LXCCores > proxmoxLXCMaxCores:
		return Exit(3, "proxmox guest=lxc requires lxcCores between 1 and %d", proxmoxLXCMaxCores)
	case p.LXCMemoryMiB < proxmoxLXCMinMemoryMiB || p.LXCMemoryMiB > proxmoxLXCMaxMemoryMiB:
		return Exit(3, "proxmox guest=lxc requires lxcMemoryMiB between %d and %d", proxmoxLXCMinMemoryMiB, proxmoxLXCMaxMemoryMiB)
	case p.LXCSwapMiB < 0 || p.LXCSwapMiB > proxmoxLXCMaxSwapMiB:
		return Exit(3, "proxmox guest=lxc requires lxcSwapMiB between 0 and %d", proxmoxLXCMaxSwapMiB)
	case p.LXCDiskGiB < proxmoxLXCMinDiskGiB || p.LXCDiskGiB > proxmoxLXCMaxDiskGiB:
		return Exit(3, "proxmox guest=lxc requires lxcDiskGiB between %d and %d", proxmoxLXCMinDiskGiB, proxmoxLXCMaxDiskGiB)
	case !proxmoxStorageIDPattern.MatchString(strings.TrimSpace(p.Storage)):
		return Exit(3, "proxmox guest=lxc requires storage for the container root disk")
	case !proxmoxBridgePattern.MatchString(strings.TrimSpace(p.Bridge)):
		return Exit(3, "proxmox guest=lxc requires bridge for the container network")
	}
	user := strings.TrimSpace(cfg.SSHUser)
	if user == "" || user == proxmoxLXCBootstrapUser || !validLinuxUserName(user) {
		return Exit(2, "proxmox guest=lxc requires a non-root lease user")
	}
	if cfg.Desktop || cfg.Browser || cfg.Code {
		return Exit(2, "provider=proxmox guest=lxc leases are headless; use a QEMU template for desktop, browser or code leases")
	}
	return nil
}

var linuxUserNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)

func validLinuxUserName(name string) bool { return linuxUserNamePattern.MatchString(name) }

func (c *ProxmoxClient) guestType() string {
	if c.Guest == ProxmoxGuestLXC {
		return ProxmoxGuestLXC
	}
	return ProxmoxGuestQEMU
}

// guestPath is the API path of one guest of the configured type.
func (c *ProxmoxClient) guestPath(vmid int) string {
	return fmt.Sprintf("/nodes/%s/%s/%d", url.PathEscape(c.Node), c.guestType(), vmid)
}

// createTaskType is the Proxmox task that creates a guest before it appears
// in cluster inventory.
func (c *ProxmoxClient) createTaskType() string {
	if c.guestType() == ProxmoxGuestLXC {
		return "vzcreate"
	}
	return "qmclone"
}

func newProxmoxLXCGeneration() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate Proxmox LXC generation: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func proxmoxLXCGenerationID(value string) string {
	if !proxmoxLXCGenerationPattern.MatchString(value) || strings.Trim(value, "0") == "" {
		return ""
	}
	return value
}

func (c *ProxmoxClient) createLXCServer(ctx context.Context, cfg Config, publicKey, leaseID, slug string, keep bool, vmid int, extraLabels map[string]string, bind func(Server) error) (Server, error) {
	if err := ValidateProxmoxGuestConfig(cfg); err != nil {
		return Server{}, err
	}
	if c.guestType() != ProxmoxGuestLXC {
		return Server{}, fmt.Errorf("proxmox client is not configured for LXC leases")
	}
	publicKey = strings.TrimSpace(publicKey)
	if publicKey == "" || strings.ContainsAny(publicKey, "\r\n") {
		return Server{}, fmt.Errorf("proxmox LXC lease requires exactly one SSH public key")
	}
	name := LeaseProviderName(leaseID, slug)
	labels := DirectLeaseLabels(cfg, leaseID, slug, "proxmox", "", keep, time.Now().UTC())
	maps.Copy(labels, extraLabels)
	labels["node"], labels["template_id"], labels["guest"] = cfg.Proxmox.Node, ProxmoxTemplateLabel(cfg), ProxmoxGuestLXC
	generation, err := newProxmoxLXCGeneration()
	if err != nil {
		return Server{}, err
	}
	labels[ProxmoxLXCGenerationLabel] = generation
	fixed := extraLabels["fixed_intent_sha256"] != ""
	if fixed && bind == nil {
		return Server{}, fmt.Errorf("fixed Proxmox container requires durable generation binding")
	}
	form := proxmoxLXCCreateForm(cfg, vmid, name, publicKey, labels)
	var upid string
	if err := c.doRequired(ctx, http.MethodPost, "/nodes/"+url.PathEscape(c.Node)+"/lxc", form, &upid); err != nil {
		return Server{}, err
	}
	createdID := strconv.Itoa(vmid)
	cleanup := func() {
		// An uncertain fixed attempt remains in custody for checked release.
		if fixed {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_ = c.DeleteServer(cleanupCtx, createdID)
	}
	if err := c.waitTask(ctx, upid); err != nil {
		cleanup()
		return Server{}, err
	}
	server, err := c.getServer(ctx, createdID, true)
	if err != nil {
		cleanup()
		return Server{}, err
	}
	if server.ImmutableID != generation {
		cleanup()
		return Server{}, fmt.Errorf("proxmox container %d does not carry the generation Crabbox created", vmid)
	}
	if fixed {
		if err := bind(server); err != nil {
			return Server{}, err
		}
	}
	// Proxmox checks tags against /vms/<vmid> alone, without the pool, so a
	// pool-scoped token may set them only once the container is a pool member.
	if err := c.configureVM(ctx, vmid, url.Values{"tags": {proxmoxLeaseTag}}); err != nil {
		cleanup()
		return Server{}, err
	}
	// Audit after binding so a refused container stays in fixed custody and
	// ordinary checked release can delete it.
	if err := c.auditLXCConfig(ctx, vmid, cfg, name); err != nil {
		cleanup()
		return Server{}, err
	}
	if err := c.startVM(ctx, vmid); err != nil {
		cleanup()
		return Server{}, err
	}
	server, err = c.waitServerIP(ctx, vmid)
	if err != nil {
		cleanup()
		return Server{}, err
	}
	if err := c.bootstrapSSH(ctx, server.PublicNet.IPv4.IP, cfg); err != nil {
		cleanup()
		return Server{}, err
	}
	server, err = c.GetServer(ctx, createdID)
	if err != nil {
		cleanup()
		return Server{}, err
	}
	return server, nil
}

// proxmoxLXCCreateForm is the complete create request. It names no feature,
// mount point, device, hookscript or raw LXC key, so the container gets
// Proxmox's default unprivileged AppArmor and seccomp confinement. Tags are
// not part of it: Proxmox checks them without the pool ACL, so they follow
// in a configuration update once the container belongs to the pool.
func proxmoxLXCCreateForm(cfg Config, vmid int, name, publicKey string, labels map[string]string) url.Values {
	p := cfg.Proxmox
	form := url.Values{
		"vmid":            {strconv.Itoa(vmid)},
		"ostemplate":      {strings.TrimSpace(p.LXCTemplate)},
		"hostname":        {name},
		"description":     {proxmoxDescription(labels)},
		"unprivileged":    {"1"},
		"ssh-public-keys": {publicKey},
		"rootfs":          {fmt.Sprintf("%s:%d", strings.TrimSpace(p.Storage), p.LXCDiskGiB)},
		"cores":           {strconv.Itoa(p.LXCCores)},
		"memory":          {strconv.Itoa(p.LXCMemoryMiB)},
		"swap":            {strconv.Itoa(p.LXCSwapMiB)},
		"net0":            {"name=eth0,bridge=" + strings.TrimSpace(p.Bridge) + ",ip=dhcp,type=veth"},
		"onboot":          {"0"},
		"start":           {"0"},
	}
	if pool := strings.TrimSpace(p.Pool); pool != "" {
		form.Set("pool", pool)
	}
	return form
}

// proxmoxLXCAllowedConfigKeys is every key a Crabbox container may carry.
// Proxmox adds arch, ostype and digest itself.
var proxmoxLXCAllowedConfigKeys = map[string]bool{
	"arch": true, "cores": true, "description": true, "digest": true, "hostname": true,
	"memory": true, "net0": true, "onboot": true, "ostype": true, "rootfs": true,
	"swap": true, "tags": true, "unprivileged": true, "lock": true,
}

// auditLXCConfig refuses a container whose configuration differs from the
// exact unprivileged, featureless shape Crabbox requested.
func (c *ProxmoxClient) auditLXCConfig(ctx context.Context, vmid int, cfg Config, name string) error {
	var config map[string]any
	if err := c.doRequired(ctx, http.MethodGet, c.guestPath(vmid)+"/config", nil, &config); err != nil {
		return err
	}
	return proxmoxLXCConfigProblem(config, cfg, name)
}

func proxmoxLXCConfigProblem(config map[string]any, cfg Config, name string) error {
	refuse := func(reason string) error {
		return fmt.Errorf("proxmox container configuration refused: %s", reason)
	}
	for key := range config {
		if !proxmoxLXCAllowedConfigKeys[key] {
			return refuse("unexpected key " + strconv.Quote(key))
		}
	}
	if _, locked := config["lock"]; locked {
		return refuse("container is locked")
	}
	if proxmoxConfigString(config["unprivileged"]) != "1" {
		return refuse("container is not unprivileged")
	}
	if proxmoxConfigString(config["tags"]) != proxmoxLeaseTag {
		return refuse("tags differ from the request")
	}
	p := cfg.Proxmox
	for key, want := range map[string]string{
		"hostname": name,
		"cores":    strconv.Itoa(p.LXCCores),
		"memory":   strconv.Itoa(p.LXCMemoryMiB),
		"swap":     strconv.Itoa(p.LXCSwapMiB),
	} {
		if proxmoxConfigString(config[key]) != want {
			return refuse(key + " differs from the request")
		}
	}
	rootfs := proxmoxConfigString(config["rootfs"])
	storage, rest, _ := strings.Cut(rootfs, ":")
	volume, _, _ := strings.Cut(rest, ",")
	if storage != strings.TrimSpace(p.Storage) || volume == "" || strings.HasPrefix(volume, "/") {
		return refuse("root disk is not a volume on the configured storage")
	}
	net0 := proxmoxConfigOptions(proxmoxConfigString(config["net0"]))
	if net0["bridge"] != strings.TrimSpace(p.Bridge) || net0["name"] != "eth0" {
		return refuse("network differs from the request")
	}
	return nil
}

func proxmoxConfigString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

func proxmoxConfigOptions(value string) map[string]string {
	options := map[string]string{}
	for _, item := range strings.Split(value, ",") {
		key, val, ok := strings.Cut(item, "=")
		if ok {
			options[strings.TrimSpace(key)] = strings.TrimSpace(val)
		}
	}
	return options
}

type proxmoxLXCInterface struct {
	Name        string `json:"name"`
	Inet        string `json:"inet"`
	IPAddresses []struct {
		Type    string `json:"ip-address-type"`
		Address string `json:"ip-address"`
	} `json:"ip-addresses"`
}

// lxcGuestIPv4 reads eth0's IPv4 address from the running container's own
// network namespace, as Proxmox reports it.
func (c *ProxmoxClient) lxcGuestIPv4(ctx context.Context, vmid int) (string, error) {
	var interfaces []proxmoxLXCInterface
	if err := c.doRequired(ctx, http.MethodGet, c.guestPath(vmid)+"/interfaces", nil, &interfaces); err != nil {
		return "", err
	}
	for _, iface := range interfaces {
		if iface.Name != "eth0" {
			continue
		}
		candidates := make([]string, 0, len(iface.IPAddresses)+1)
		for _, addr := range iface.IPAddresses {
			if addr.Type == "inet" {
				candidates = append(candidates, addr.Address)
			}
		}
		if address, _, ok := strings.Cut(iface.Inet, "/"); ok {
			candidates = append(candidates, address)
		}
		for _, candidate := range candidates {
			ip := net.ParseIP(strings.TrimSpace(candidate))
			if ip != nil && ip.To4() != nil && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
				return ip.String(), nil
			}
		}
	}
	return "", errors.New("no container ipv4 address reported for eth0")
}

// proxmoxLXCBootstrapScript runs once as root over the create-time key. The
// template owns every package: this checks them, installs nothing, moves the
// per-lease key to the lease user and disables root login for good.
func proxmoxLXCBootstrapScript(cfg Config) string {
	return fmt.Sprintf(`set -euo pipefail
user=%[1]s
work_root=%[2]s
for tool in sshd sudo git rsync curl jq runuser; do
  command -v "$tool" >/dev/null || { echo "proxmox lxc template is missing $tool" >&2; exit 1; }
done
uid=$(id -u "$user" 2>/dev/null) || { echo "proxmox lxc template has no user $user" >&2; exit 1; }
[ "$uid" != 0 ] || { echo "proxmox lxc lease user must not be root" >&2; exit 1; }
home=$(getent passwd "$user" | cut -d: -f6)
case "$home" in /home/*) ;; *) echo "proxmox lxc lease user has an unexpected home" >&2; exit 1 ;; esac
test -s /root/.ssh/authorized_keys
install -d -m 0700 -o "$user" -g "$user" "$home/.ssh"
install -m 0600 -o "$user" -g "$user" /root/.ssh/authorized_keys "$home/.ssh/authorized_keys"
mkdir -p "$work_root" /var/cache/crabbox/pnpm /var/cache/crabbox/npm /var/lib/crabbox
chown -R "$user:$user" "$work_root" /var/cache/crabbox
cat >/usr/local/bin/crabbox-ready <<'READY'
#!/usr/bin/env bash
set -euo pipefail
git --version >/dev/null
rsync --version >/dev/null
curl --version >/dev/null
jq --version >/dev/null
test -w %[2]s
READY
chmod 0755 /usr/local/bin/crabbox-ready
# Root is only the create-time handoff. Every later session is the lease user.
install -d -m 0755 /etc/ssh/sshd_config.d
printf 'PermitRootLogin no\n' >/etc/ssh/sshd_config.d/00-crabbox-no-root.conf
rm -f /root/.ssh/authorized_keys
sshd -t
systemctl restart ssh.service
runuser -u "$user" -- /usr/local/bin/crabbox-ready
touch /var/lib/crabbox/bootstrapped
`, shellQuote(cfg.SSHUser), shellQuote(cfg.WorkRoot))
}

// bootstrapLXCSSH runs the root handoff. Its diagnostics follow the QEMU
// bootstrap: bounded, redacted, and never shown when truncated.
func (c *ProxmoxClient) bootstrapLXCSSH(ctx context.Context, host string, cfg Config) error {
	target := SSHTargetFromConfig(cfg, host)
	target.User = proxmoxLXCBootstrapUser
	deadline := time.Now().Add(10 * time.Minute)
	for {
		if proxmoxRunSSHQuietWithOptions(ctx, target, sshTransportProbeCommand(target), "5", "1") == nil {
			out := newSynchronizedBuffer(proxmoxBootstrapDiagnosticLimit)
			err := proxmoxRunSSHInput(ctx, target, "/bin/bash -s", strings.NewReader(proxmoxLXCBootstrapScript(cfg)), &out, &out)
			if err == nil {
				return nil
			}
			return c.proxmoxBootstrapFailure(err, &out, cfg)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for proxmox ssh bootstrap transport")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func (c *ProxmoxClient) proxmoxBootstrapFailure(err error, out *synchronizedBuffer, cfg Config) error {
	status := "unknown"
	var native *exec.ExitError
	if errors.As(err, &native) && native.ExitCode() >= 0 {
		status = strconv.Itoa(native.ExitCode())
	}
	diagnostic, truncated := out.boundedString()
	if truncated {
		// A cut credential cannot be reliably redacted from a partial capture.
		diagnostic = "diagnostics truncated; captured output omitted"
	}
	message := fmt.Sprintf("proxmox guest bootstrap exit=%s: %v", status, err)
	if diagnostic = strings.TrimSpace(diagnostic); diagnostic != "" {
		message += ": " + diagnostic
	}
	message = RedactDiagnosticSecrets(message, c.TokenID, c.TokenSecret, cfg.Proxmox.TokenID, cfg.Proxmox.TokenSecret)
	message = proxmoxAPITokenPattern.ReplaceAllString(message, "PVEAPIToken=<redacted>")
	return &proxmoxBootstrapError{message: message, cause: err}
}

type proxmoxVolume struct {
	VolID   string `json:"volid"`
	Content string `json:"content"`
}

// proxmoxLXCTemplateCheck confirms that the configured vztmpl volume is
// visible to this principal. It reads storage content only.
func (c *ProxmoxClient) proxmoxLXCTemplateCheck(ctx context.Context, cfg Config) ProxmoxReadinessCheck {
	volid := strings.TrimSpace(cfg.Proxmox.LXCTemplate)
	if !proxmoxLXCTemplatePattern.MatchString(volid) {
		return ProxmoxReadinessCheck{
			Status: "failed", Check: "template",
			Message: "lxcTemplate=missing class=config hint=set_proxmox_lxc_template",
			Details: map[string]string{"lxcTemplate": "missing", "class": "config", "hint": "set_proxmox_lxc_template"},
		}
	}
	storage, _, _ := strings.Cut(volid, ":")
	path := fmt.Sprintf("/nodes/%s/storage/%s/content?content=vztmpl", url.PathEscape(c.Node), url.PathEscape(storage))
	var volumes []proxmoxVolume
	if err := c.doRequired(ctx, http.MethodGet, path, nil, &volumes); err != nil {
		return c.proxmoxFailedReadiness("template", path, err, map[string]string{"lxcTemplate": volid})
	}
	for _, volume := range volumes {
		if volume.VolID == volid && volume.Content == "vztmpl" {
			return ProxmoxReadinessCheck{
				Status: "ok", Check: "template",
				Message: fmt.Sprintf("lxcTemplate=%s template=ready", volid),
				Details: map[string]string{"lxcTemplate": volid, "template": "ready", "endpoint": proxmoxDisplayPath(path)},
			}
		}
	}
	return ProxmoxReadinessCheck{
		Status: "failed", Check: "template",
		Message: fmt.Sprintf("lxcTemplate=%s class=missing_resource hint=upload_proxmox_lxc_template", volid),
		Details: map[string]string{"lxcTemplate": volid, "class": "missing_resource", "hint": "upload_proxmox_lxc_template", "endpoint": proxmoxDisplayPath(path)},
	}
}

// proxmoxLXCPrivileges are what creating, starting, inspecting and deleting
// a bounded container needs on its pool, or on /vms without a pool.
// VM.Config.Options also covers the crabbox tag, which is applied after the
// container has joined the pool.
var proxmoxLXCPrivileges = []string{
	"VM.Allocate", "VM.Audit", "VM.Config.CPU", "VM.Config.Disk", "VM.Config.Memory",
	"VM.Config.Network", "VM.Config.Options", "VM.PowerMgmt",
}

// proxmoxLXCPermissionCheck reports missing privileges before the first
// create, without creating anything.
func (c *ProxmoxClient) proxmoxLXCPermissionCheck(ctx context.Context, cfg Config) ProxmoxReadinessCheck {
	scope := "/vms"
	if pool := strings.TrimSpace(cfg.Proxmox.Pool); pool != "" {
		scope = "/pool/" + pool
	}
	storage := strings.TrimSpace(cfg.Proxmox.Storage)
	templateStorage, _, _ := strings.Cut(strings.TrimSpace(cfg.Proxmox.LXCTemplate), ":")
	bridge := strings.TrimSpace(cfg.Proxmox.Bridge)
	required := []struct {
		path  string
		privs []string
		any   bool
	}{
		{path: scope, privs: proxmoxLXCPrivileges},
		{path: "/storage/" + storage, privs: []string{"Datastore.AllocateSpace"}},
		{path: "/storage/" + templateStorage, privs: []string{"Datastore.AllocateSpace", "Datastore.Audit"}, any: true},
		// Attaching net0 to a plain bridge is checked on its local SDN zone.
		{path: "/sdn/zones/localnetwork/" + bridge, privs: []string{"SDN.Use"}},
		// Prepared-claim recovery inspects the node's active vzcreate tasks.
		{path: "/nodes/" + strings.TrimSpace(cfg.Proxmox.Node), privs: []string{"Sys.Audit"}},
	}
	var missing []string
	for _, requirement := range required {
		path := "/access/permissions?path=" + url.QueryEscape(requirement.path)
		var permissions map[string]map[string]proxmoxInt
		if err := c.doRequired(ctx, http.MethodGet, path, nil, &permissions); err != nil {
			return c.proxmoxFailedReadiness("lxc_permissions", path, err, map[string]string{"path": requirement.path})
		}
		held := permissions[requirement.path]
		found := 0
		for _, priv := range requirement.privs {
			if _, ok := held[priv]; ok {
				found++
			} else if !requirement.any {
				missing = append(missing, requirement.path+":"+priv)
			}
		}
		if requirement.any && found == 0 {
			missing = append(missing, requirement.path+":"+strings.Join(requirement.privs, "|"))
		}
	}
	if len(missing) > 0 {
		return ProxmoxReadinessCheck{
			Status: "failed", Check: "lxc_permissions",
			Message: "lxc_permissions=missing class=permission hint=grant_proxmox_lxc_lease_privileges missing=" + strings.Join(missing, ","),
			Details: map[string]string{"class": "permission", "hint": "grant_proxmox_lxc_lease_privileges", "missing": strings.Join(missing, ",")},
		}
	}
	return ProxmoxReadinessCheck{
		Status: "ok", Check: "lxc_permissions",
		Message: "lxc_permissions=ready scope=" + scope,
		Details: map[string]string{"scope": scope, "lxc_permissions": "ready"},
	}
}

// proxmoxLXCStorageCheck needs root-disk storage for containers and a
// template storage that holds vztmpl archives.
func proxmoxLXCStorageCheck(cfg Config, storages []proxmoxStorage, endpoint string) ProxmoxReadinessCheck {
	check := proxmoxNamedStorageReadiness(strings.TrimSpace(cfg.Proxmox.Storage), "rootdir", storages, endpoint)
	if check.Status != "ok" {
		return check
	}
	templateStorage, _, _ := strings.Cut(strings.TrimSpace(cfg.Proxmox.LXCTemplate), ":")
	source := proxmoxNamedStorageReadiness(templateStorage, "vztmpl", storages, endpoint)
	if source.Status != "ok" {
		source.Details["source"] = "lxcTemplate"
		return source
	}
	check.Details["source"] = "configured"
	check.Details["templateStorages"] = templateStorage
	return check
}

// lxcStopIfRunning stops a container only when Proxmox reports it running.
// Proxmox refuses to stop a stopped container, so a refused container that
// never started, or one that exited on its own, would otherwise never pass
// checked release. Any other state fails closed.
func (c *ProxmoxClient) lxcStopIfRunning(ctx context.Context, vmid int) error {
	var status proxmoxVM
	if err := c.doRequired(ctx, http.MethodGet, c.guestPath(vmid)+"/status/current", nil, &status); err != nil {
		return err
	}
	switch status.Status {
	case "stopped":
		return nil
	case "running":
		var upid string
		if err := c.do(ctx, http.MethodPost, c.guestPath(vmid)+"/status/stop", url.Values{}, &upid); err != nil {
			if IsProxmoxNotFound(err) {
				return nil
			}
			return err
		}
		return c.waitTask(ctx, upid)
	default:
		return fmt.Errorf("proxmox container %d is in state %q; refusing to stop or delete it", vmid, status.Status)
	}
}

// lxcPreserveGeneration keeps the generation label a container was created
// with. The label lives in the description, which every label write
// replaces, so a writer that omits it would erase the container's identity.
// A different generation is refused.
func (c *ProxmoxClient) lxcPreserveGeneration(ctx context.Context, vmid int, labels map[string]string) (map[string]string, error) {
	var config map[string]any
	if err := c.doRequired(ctx, http.MethodGet, c.guestPath(vmid)+"/config", nil, &config); err != nil {
		return nil, err
	}
	current := ""
	if desc, ok := config["description"].(string); ok {
		current = proxmoxLXCGenerationID(proxmoxDescriptionLabels(desc)[ProxmoxLXCGenerationLabel])
	}
	if current == "" {
		return labels, nil
	}
	merged := maps.Clone(labels)
	if merged == nil {
		merged = map[string]string{}
	}
	switch merged[ProxmoxLXCGenerationLabel] {
	case "":
		merged[ProxmoxLXCGenerationLabel] = current
	case current:
	default:
		return nil, fmt.Errorf("proxmox container %d generation label does not match the container", vmid)
	}
	return merged, nil
}
