import { base64URL } from "./encoding";

// A guest may front its VNC server with an input gate (see the Go bridge's
// webvnc_input_gate.go). The gate lets desktop input through only for the
// viewer session that holds control, and keeps an agent inside the guest out.
// The coordinator is the only party that knows which viewer session a bridge
// connection serves, so it tells the gate: once per connection, as an opaque
// binding derived from the lease and the viewer session. Take and return
// requests come from an authenticated viewer through the coordinator; a
// viewer's own input_* frames are reserved and never reach the bridge.
//
// Every answer states its outcome. "confirmed" and "refused" come from the
// gate itself. "unknown" means the gate may have committed the change but
// its answer was lost: the connection closed or the gate did not answer in
// time. The viewer then reconnects and shows the state the gate reports,
// instead of saying that control did not change.

export const webVNCInputGateCapability = "input_gate";

const bindingDomain = "crabbox-webvnc-input-binding/1";
const requestTimeoutMs = 45_000;
const owners = new Set(["agent", "human", "none"]);
const holders = new Set(["self", "other", "none"]);

// User-facing text is written here, never taken from the guest.
const refusalMessages: Record<string, string> = {
  held_by_other: "Another viewer has control of this desktop.",
  viewer_unbound: "This viewer is not bound to a viewer session. Reconnect and try again.",
  viewer_disconnected: "The viewer disconnected while control was changing.",
  lease_mismatch: "This desktop belongs to a different lease.",
  not_initialized: "Desktop input has not been set up on this computer.",
  agent_not_stopped: "The agent's desktop input did not stop in time. Nobody has control.",
  desktop_not_cleared: "Programs on the desktop did not stop in time. Nobody has control.",
  take_failed: "Control could not be taken safely. Nobody has control.",
  release_failed: "Your input could not be released safely. Nobody has control.",
  busy: "A change of control is already in progress.",
  bridge_disconnected:
    "The desktop connection closed before the request was sent. Control did not change.",
};

// The answer to a request the gate received was lost; control may have changed.
const unknownMessages: Record<string, string> = {
  bridge_disconnected:
    "The desktop connection closed while control was changing. Reconnecting to check who has control.",
  timeout: "The desktop did not answer in time. Checking who has control.",
};

export type WebVNCInputAction = "take" | "return";

/**
 * Take and return change who may give a desktop input, so they accept only a
 * same-origin JSON request from the viewer page itself: the exact Origin of
 * the coordinator, and a JSON body. This holds even for callers the portal's
 * cookie gate would let through.
 */
export function webVNCInputRequestRefusal(
  request: Request,
  trustedOrigin: string,
): { status: number; error: string; message: string } | undefined {
  const origin = request.headers.get("origin");
  if (origin === null || origin !== trustedOrigin) {
    return {
      status: 403,
      error: "input_origin_forbidden",
      message: "take and return must come from the viewer page",
    };
  }
  const type = (request.headers.get("content-type") ?? "").trim().toLowerCase();
  if (type !== "application/json" && type !== "application/json; charset=utf-8") {
    return {
      status: 415,
      error: "unsupported_media_type",
      message: "take and return need a JSON body",
    };
  }
  return undefined;
}

export interface WebVNCInputView {
  gate: boolean;
  /** Absent until the gate has reported its state on this connection. */
  owner?: "agent" | "human" | "none";
  holder?: "self" | "other" | "none";
}

type InputState = { owner: "agent" | "human" | "none"; holder: "self" | "other" | "none" };

export type WebVNCInputResult =
  | ({ ok: true; outcome: "confirmed" } & InputState)
  | ({ ok: false; outcome: "refused"; code: string; message: string } & Partial<InputState>)
  | { ok: false; outcome: "unknown"; code: string; message: string };

interface Pending {
  key: string;
  resolve(result: WebVNCInputResult): void;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * The opaque value the gate uses to tell viewers apart. It is stable for one
 * key on one lease, so a reconnect resumes control, and reveals nothing about
 * the key or any cookie.
 */
export async function webVNCInputBinding(leaseID: string, sessionKey: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(`${bindingDomain}\n${leaseID}\n${sessionKey}`),
  );
  return base64URL(new Uint8Array(digest));
}

/**
 * The key a viewer's binding comes from. A bootstrapped viewer session
 * carries its browser's ID, which outlives the session, so a reload or a new
 * session in the same browser keeps control. A session stored before
 * browser IDs keys on itself, and a viewer without a session is its own key
 * for this connection only.
 */
export function webVNCViewerInputKey(
  viewerSession: { session: string; browser?: string } | undefined,
  owner: string,
  viewerID: string,
): string {
  if (!viewerSession) {
    return `viewer:${owner}:${viewerID}`;
  }
  return viewerSession.browser
    ? `browser:${viewerSession.browser}`
    : `session:${viewerSession.session}`;
}

function parseGateMessage(message: unknown): Record<string, unknown> | undefined {
  if (typeof message !== "string" || message.length > 4096 || message[0] !== "{") {
    return undefined;
  }
  try {
    const parsed = JSON.parse(message) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return undefined;
    }
    const record = parsed as Record<string, unknown>;
    return typeof record["type"] === "string" && record["type"].startsWith("input_")
      ? record
      : undefined;
  } catch {
    return undefined;
  }
}

function stateOf(record: Record<string, unknown>): InputState | undefined {
  const owner = record["owner"];
  const holder = record["holder"];
  if (typeof owner !== "string" || !owners.has(owner)) return undefined;
  if (typeof holder !== "string" || !holders.has(holder)) return undefined;
  return { owner: owner as InputState["owner"], holder: holder as InputState["holder"] };
}

function inputKey(leaseID: string, agentID: string): string {
  return `${leaseID}:${agentID}`;
}

/** Tracks each gate connection's reported state and in-flight requests. */
export class WebVNCInputTracker {
  private readonly states = new Map<string, InputState>();
  private readonly pending = new Map<string, Pending>();

  /** Handles a bridge's input_state or input_result message; false for anything else. */
  receive(leaseID: string, agentID: string, message: unknown): boolean {
    const record = parseGateMessage(message);
    if (!record) return false;
    const key = inputKey(leaseID, agentID);
    const state = stateOf(record);
    if (record["type"] === "input_state") {
      if (state) this.states.set(key, state);
      return true;
    }
    if (record["type"] === "input_result" && typeof record["id"] === "string") {
      const pending = this.pending.get(record["id"]);
      if (!pending || pending.key !== key) return true;
      this.pending.delete(record["id"]);
      clearTimeout(pending.timer);
      if (state) this.states.set(key, state);
      const code = typeof record["code"] === "string" ? record["code"] : "take_failed";
      pending.resolve(
        record["ok"] === true && state
          ? { ok: true, outcome: "confirmed", ...state }
          : {
              ok: false,
              outcome: "refused",
              ...(state ?? { owner: "none", holder: "none" }),
              code: code in refusalMessages ? code : "refused",
              message: refusalMessages[code] ?? "Control did not change.",
            },
      );
    }
    return true;
  }

  view(leaseID: string, agentID: string | undefined, gate: boolean): WebVNCInputView {
    if (!gate || !agentID) return { gate: false };
    return { gate: true, ...this.states.get(inputKey(leaseID, agentID)) };
  }

  /** Sends a take or return request down one gate connection and waits for its answer. */
  request(
    leaseID: string,
    agentID: string,
    socket: WebSocket,
    action: WebVNCInputAction,
  ): Promise<WebVNCInputResult> {
    const key = inputKey(leaseID, agentID);
    const bytes = new Uint8Array(12);
    crypto.getRandomValues(bytes);
    const id = base64URL(bytes);
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        resolve(unknown("timeout"));
      }, requestTimeoutMs);
      this.pending.set(id, { key, resolve, timer });
      try {
        socket.send(JSON.stringify({ type: "input_request", id, action }));
      } catch {
        // Never sent, so control certainly did not change.
        this.pending.delete(id);
        clearTimeout(timer);
        resolve({
          ok: false,
          outcome: "refused",
          ...this.states.get(key),
          code: "bridge_disconnected",
          message: refusalMessages["bridge_disconnected"]!,
        });
      }
    });
  }

  /**
   * Forgets a closed gate connection. Its in-flight requests reached the
   * gate, which may have committed them, so their outcome is unknown.
   */
  forget(leaseID: string, agentID: string): void {
    const key = inputKey(leaseID, agentID);
    this.states.delete(key);
    for (const [id, pending] of this.pending) {
      if (pending.key !== key) continue;
      this.pending.delete(id);
      clearTimeout(pending.timer);
      pending.resolve(unknown("bridge_disconnected"));
    }
  }
}

function unknown(code: string): WebVNCInputResult {
  return { ok: false, outcome: "unknown", code, message: unknownMessages[code]! };
}
