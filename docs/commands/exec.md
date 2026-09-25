# exec

`crabbox exec` executes a command on a supported existing lease without syncing files,
hydrating a workspace, or changing its working directory. It resolves fresh SSH
access and keeps the current repository claim held until the command and local
transport cleanup finish.

```sh
crabbox exec --id cbx_abcdef123456 -- /bin/sh -c 'cd /workspace && make test' </dev/null
printf 'file contents' | crabbox exec --id cbx_abcdef123456 -- /bin/sh -c 'cat > /workspace/example.txt'
printf 'pwd\nexit\n' | crabbox exec --id cbx_abcdef123456 --pty -- /bin/sh
```

Use `run` when Crabbox should own workspace sync, hydration, logs, and results.
Use `exec` when the caller already owns those policies and needs a command
transport. Command arguments are literal; use `/bin/sh -c` explicitly for shell
syntax. The remote SSH account's initial directory remains unchanged.

## Ownership and lifecycle

Before creating a fixed-ID lease, integrations can check the configured route:

```sh
crabbox exec --check --provider daytona
```

```json
{"provider":"daytona","target":"linux","execution":true,"currentRepoStop":true}
```

This reads configuration and compiled provider capabilities without configuring a
backend, authenticating, inspecting claims, or contacting a provider. `execution`
reports support for this command on completed fixed-ID leases of the selected target. `currentRepoStop`
reports support for repository-scoped cleanup of **fixed-ID** leases. Integrations
that require both must reject either false value before allocating. These flags
do not prove credentials, connectivity, capacity, or an existing lease's state.
`--check` cannot combine an ID, command, `--pty`, or supervisor options.

Direct Daytona initially supports both capabilities for completed fixed-ID leases
through its existing native activity renewal and fixed-lease release fence.
Direct Proxmox supports execution for completed fixed-ID leases through its
fixed-lease release fence; it does not renew idle activity. Ordinary leases and
incomplete fixed acquisitions are rejected before native access preparation; use
`run` for ordinary leases. Other providers, including AWS and Machine0, and
coordinator routes report both unavailable. The underlying `claim-exec` and `fixed-current-repo-stop`
feature names also appear in `crabbox providers --json`.

Run from the repository that currently owns the lease. `--id` requires the
canonical Crabbox lease ID and an existing resource-bound repository claim of a
kind admitted by the provider's execution owner.
Provider routing uses that stored claim unless explicitly overridden. A
different repository is rejected before native access preparation. `exec` does
not acquire, adopt, reclaim, or release a lease.

Crabbox holds its shared claim fence through fresh access resolution, execution,
and cleanup. Other commands on that same claim may run concurrently. Claim
writers, including reclaim and release, wait for execution to finish. Provider
activity renewal runs where supported; it does not extend the maximum lease
lifetime. Cancellation terminates and joins the owned local SSH process tree
before releasing the fence. Remote process termination follows the SSH server's
session behavior; detached remote processes are outside this command's lifetime
unless `--terminate-remote-on-disconnect` is set.

SSH credentials remain in private temporary configuration, never in printed
commands or child arguments. The temporary configuration is removed after the
SSH child exits. No command is replayed after transport failure.

## Streams and supported targets

Without `--pty`, stdin, stdout, and command stderr are forwarded without buffering
the entire command or modifying their bytes. Crabbox returns the SSH command's
exit code. Provider or transport setup errors may produce diagnostics on stderr.

`--pty` requests a remote pseudo-terminal and therefore uses normal terminal
semantics, including merged remote output and terminal line processing. It is
not a byte-preserving file-transfer mode. The caller supplies input through a
pipe and owns any local terminal modes or resizing; `exec` is a command transport,
not a local interactive terminal UI. A controlling-terminal stdin is rejected;
redirect from `/dev/null` when the command needs no input.

The transport supports Linux and macOS SSH targets whose provider implements
execution-specific claim admission; initial support is Daytona fixed-ID Linux
leases. Unsupported providers and Windows targets
are rejected; there is no raw-credential fallback. Provider-native proxy routes
are retained through Crabbox's private SSH transport.

## Supervisor options

A trusted supervisor that returns results to another principal, such as the
runtime adapter's [workspace exec](../features/runtime-adapter-exec.md), can
use three further options. None can be combined with `--check`.
`--status-fd` and `--terminate-remote-on-disconnect` cannot be combined with
`--pty`, because a remote terminal echoes input into output.

- `--status-fd <n>` writes JSON status lines to an inherited descriptor of at
  least 3. `{"event":"started"}` is written once the SSH command has started,
  so any other nonzero exit is a Crabbox setup failure rather than the remote
  command's status. Provider preparation diagnostics are kept out of stderr in
  this mode. The descriptor is not inherited by the SSH transport.
- `--expect-runtime-registration <id>` requires the lease claim to hold exactly
  this acknowledged runtime adapter registration generation, with no pending
  replacement. It is checked under the shared claim fence before provider
  access, so a rotation or release cannot interleave with the command. A
  mismatch exits 4 and, with `--status-fd`, writes
  `{"event":"rejected","reason":"registration"}`.
- `--terminate-remote-on-disconnect` runs the command in its own remote process
  group and terminates that group, with `TERM` and then `KILL` after five
  seconds, once the SSH session that started it has ended. Without a PTY,
  sshd does not signal remote commands when the client goes away. The remote
  host needs a POSIX `sh`; `setsid` is used when available, otherwise only the
  direct child is signalled. Processes that start their own session remain
  outside this guard.

## Flags

```text
--id <canonical-lease-id>  Existing, currently owned lease.
--check                    Print effective capabilities offline as JSON.
--pty                      Allocate a remote pseudo-terminal.
--provider <name>          Explicit provider override; defaults to stored routing.
--network auto|tailscale|public
--status-fd <n>            Supervisor status descriptor (3 or higher).
--expect-runtime-registration <id>
                           Required runtime adapter registration generation.
--terminate-remote-on-disconnect
                           Stop the remote process group when the session ends.
```

Provider-specific routing flags are the same as [`ssh`](ssh.md).

## See also

- [`run`](run.md) — synchronize, hydrate, execute, and collect results.
- [`connect`](connect.md) — open an interactive SSH session.
- [`stop --current-repo`](stop.md) — release a fixed-ID lease only while its current claim belongs to the calling repository.
