package cli

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	xssh "golang.org/x/crypto/ssh"
)

// ProxmoxSSHHostKeyLabel records a QEMU lease's own SSH host key in the VM's
// Proxmox description, where the guest cannot change it.
const ProxmoxSSHHostKeyLabel = "ssh_host_key"

const proxmoxGuestHostKeyPath = "/etc/ssh/ssh_host_ed25519_key.pub"

// GuestSSHHostKey reads a QEMU guest's SSH host key through the Proxmox guest
// agent. The agent channel is a virtio port the hypervisor binds to this one
// VM, so no other guest or network peer can answer for it, unlike the first
// key an SSH connection happens to see. It returns the key in authorized-keys
// form, or an error while the guest has not yet generated one.
func (c *ProxmoxClient) GuestSSHHostKey(ctx context.Context, vmid int) (string, error) {
	var res struct {
		Content   string `json:"content"`
		Truncated bool   `json:"truncated"`
	}
	path := fmt.Sprintf("/nodes/%s/qemu/%d/agent/file-read?file=%s", url.PathEscape(c.Node), vmid, url.QueryEscape(proxmoxGuestHostKeyPath))
	if err := c.doRequired(ctx, http.MethodGet, path, nil, &res); err != nil {
		return "", err
	}
	if res.Truncated {
		return "", fmt.Errorf("proxmox guest agent returned a truncated SSH host key")
	}
	key, _, options, rest, err := xssh.ParseAuthorizedKey([]byte(strings.TrimSpace(res.Content) + "\n"))
	if err != nil || len(options) != 0 || len(strings.TrimSpace(string(rest))) != 0 || key.Type() != xssh.KeyAlgoED25519 {
		return "", fmt.Errorf("proxmox guest has no ed25519 SSH host key yet")
	}
	return strings.TrimSpace(string(xssh.MarshalAuthorizedKey(key))), nil
}

// waitGuestSSHHostKey waits until the guest has generated its host key, which
// cloud-init does once per instance at first boot.
func (c *ProxmoxClient) waitGuestSSHHostKey(ctx context.Context, vmid int) (string, error) {
	deadline := time.Now().Add(10 * time.Minute)
	for {
		key, err := c.GuestSSHHostKey(ctx, vmid)
		if err == nil {
			return key, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("timeout reading proxmox guest SSH host key through the guest agent: %w", err)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

// PinProxmoxHostKey pins an SSH target to the host key recorded when the
// lease was created. The key is checked strictly, under a per-lease alias in
// the lease's own known_hosts, so trust follows the lease and never the
// address. Leases without a recorded key, such as LXC leases, are unchanged
// and report no pinned identity.
func PinProxmoxHostKey(target *SSHTarget, server Server, leaseID string) error {
	key := strings.TrimSpace(server.Labels[ProxmoxSSHHostKeyLabel])
	if key == "" {
		return nil
	}
	target.SSHHostKey = key
	return prepareLeaseSSHTrust(target, leaseID)
}
