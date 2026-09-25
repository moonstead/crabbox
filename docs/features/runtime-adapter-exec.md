# Runtime adapter workspace exec

Read this when you:

- need to run a command, with private input, on a workspace behind
  `crabbox adapter serve` and `adapter connect`;
- decide whether to enable that capability on an adapter host;
- change the exec route in `internal/cli/controller_exec.go`,
  `internal/cli/adapter_connect.go` or `worker/src/runtime-adapter-relay.ts`.

Workspace exec lets the owner of a ready workspace run one command in it and
receive its exit status and bounded output. A typical use is a one-time agent
enrolment step that reads a credential from stdin before the guest has its own
connection. Provider credentials and per-lease SSH keys stay on the adapter
host: the adapter runs [`crabbox exec`](../commands/exec.md) itself.

Exec is off unless every layer opts in. It is a command channel into a guest the
caller already owns, not a shell on the adapter host, a terminal, a file
transfer or a general tunnel.

## Opt-in at every layer

| Layer | Opt-in | Without it |
| --- | --- | --- |
| Adapter host | `adapter serve --exec-allow '<JSON argv prefix>'` | Route returns `404`; `capabilities.exec` is rejected at creation |
| Provider | `crabbox exec --check` reports `execution: true`, checked at startup | `adapter serve` refuses to start with `--exec-allow` |
| Workspace | created with `"capabilities": {"exec": true}` | `403 exec_not_enabled` |
| Relay | `adapter connect --allow-exec` advertises an exec budget in its ticket | Connector rejects exec frames; coordinator returns `409 runtime_adapter_exec_unavailable` without dispatch |

`--exec-allow` is repeatable and flag-only. Each value is a JSON array that an
argv must start with, element by element. The operator therefore decides which
programs callers may start. Treat a prefix such as `["sh","-c"]` or
`["sh","-s","--"]` as arbitrary code execution inside the guest, as the
guest's SSH user. That is no more than a desktop session in the same guest
allows, but it is a programmatic channel with private input, so enable it only
where the workspace owner is trusted with the guest.

The provider must support `crabbox exec` on completed fixed-ID leases, which
`adapter serve` always creates. Today that is Proxmox and Daytona.

## Request and response

Through the coordinator:

```http
POST /v1/adapters/{adapter-id}/proxy/v1/workspaces/{workspace-id}/exec
Content-Type: application/json

{"argv":["sh","-c","..."],"stdinBase64":"...","timeoutMs":600000}
```

Directly on `adapter serve`, callers must also send `leaseId`, the workspace's
current lease, and may send `registrationId`. The coordinator fills both from
its own record and rejects conflicting values supplied by the caller.

A completed command returns `200`:

```json
{"exitCode":0,"stdoutBase64":"...","stderrBase64":"...","stdoutBytes":120,"stderrBytes":0,"stdoutTruncated":false,"stderrTruncated":false,"durationMs":5120}
```

`exitCode` is the remote command's exit status. As with any SSH command, `255`
can also mean the SSH transport failed after the command started. If Crabbox
could not start the command at all, the response is `502 exec_unavailable` and
Crabbox's own diagnostics are not returned. Output is delivered when the command
ends; it is not streamed.

| Limit | Value |
| --- | --- |
| Request body | 256 KiB |
| Decoded stdin | 128 KiB |
| argv | 1 to 256 arguments, 32 KiB in total, UTF-8 without NUL |
| Output | the last 16 KiB of each of stdout and stderr, with total byte counts and truncation flags |
| `timeoutMs` | 1 second to `--exec-max-timeout` (default 15 minutes, at most 1 hour) |
| Concurrency | one command per workspace; at most `--max-concurrent` per adapter, separate from lifecycle work; four in flight per adapter through the relay |

Exec is never retried by the coordinator or the connector.

## Authorisation at execution time

The coordinator admits a command only when:

- the caller owns the adapter identity, with no administrator bypass;
- exactly one live registered lease is bound to that adapter and workspace;
- the caller owns that lease;
- the lease is active, unexpired and not being deleted.

Admission and dispatch run in the same exclusive section that records a
deletion, so a command cannot be dispatched after its workspace's deletion has
been recorded. The relayed request carries the lease ID and registration
generation. Its deadline is the earliest of the requested timeout plus setup
time, the connector's advertised budget and the lease expiry.

The connector accepts exec frames only when it opted in, and only with a
canonical lease ID and registration ID.

`adapter serve` then requires the workspace to have the exec capability, match
the lease ID, be `ready` and unexpired, have no local cleanup pending, and have
durable state. The `crabbox exec` child checks the registration generation
against the local claim under the shared claim fence, before any provider or
SSH access. Registration rotation and release are claim writers, so they cannot
interleave with the command.

## Private input

Stdin travels as base64 in the request body. It is decoded in memory and handed
to the `crabbox exec` child only through an inherited pipe. The coordinator,
connector and adapter do not store, log or quote it:

- never in argv, environment, URLs, adapter state, coordinator records or
  lease labels;
- never in error messages, including decoder errors for malformed bodies;
- never in the adapter's audit line.

Argv is not private. It is visible in process listings on the adapter host and
in the guest, so callers must put secrets only in stdin. The guest command
itself must not echo its input; Crabbox cannot prevent a command from printing
what it reads.

## Cancellation, revocation and stale output

The adapter-owned process tree (the `crabbox exec` child and its SSH transport)
is terminated and joined when any of the following happens:

- DELETE, or lease release with deletion through the coordinator;
- adapter or provider expiry, including a stopping transition that could not be
  persisted;
- a state durability barrier;
- the caller disconnects: the coordinator sends a `cancel` frame for that
  request;
- the timeout or lease expiry deadline passes;
- the relay WebSocket disconnects or `adapter serve` shuts down.

The adapter runs the command with `--terminate-remote-on-disconnect`, so the
remote process group is stopped once the SSH session ends. Processes that start
their own session, such as services installed by the command, remain outside
this guard. Release deletes the guest anyway.

A result is released only after a final check that runs under the same gate as
DELETE: the workspace must still be ready on the same lease and unexpired, state
must be durable and the caller still connected. The coordinator checks again
that the lease generation is still current before returning a successful
response. Otherwise the output is withheld and the caller receives `409` (or
`503` for a durability barrier, `504` for a timeout).

## Audit

`adapter serve` writes one metadata-only line per exec attempt to its log:

```text
controller exec workspace=<id> lease=<lease> registration=<id|-> policy=<prefix index> argc=<n> argv_sha256=<digest> stdin_bytes=<n> outcome=<outcome> exit=<code|-> stdout_bytes=<n> stderr_bytes=<n> duration=<d>
```

It records neither argv, stdin nor output. The argv digest covers the NUL-joined
arguments; do not rely on it to hide a low-entropy value placed in argv.

## Relay protocol additions

The connector's ticket request adds `execTimeoutMs`, its local exec budget plus
response delivery time, only when it opted in. The coordinator records it on the
connection and dispatches exec only to connections that advertised it. It adds
one frame type from coordinator to connector:

```json
{"type":"cancel","id":"<request id>"}
```

The connector cancels that in-flight request; unknown IDs are ignored. A
withdrawn request stays counted against relay capacity until the connector
answers or the deadline passes.

## Compatibility

Exec is additive and off by default. An older connector never advertises exec,
so a newer coordinator never dispatches it. An older coordinator does not know
the route and returns `404`. A state file containing a workspace created with
`capabilities.exec` cannot be read by an older `adapter serve`; downgrade only
after those workspaces are gone.

## Related

- [`adapter` command](../commands/adapter.md)
- [`exec` command](../commands/exec.md)
- [Runtime adapter stack](runtime-adapter-stack.md)
- [Proxmox provider](../providers/proxmox.md)
