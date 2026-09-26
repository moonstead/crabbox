import { describe, expect, it, vi } from "vitest";

import { FleetCoordinator, isReservedWebVNCControlFrame } from "../src/fleet";
import { portalVNC } from "../src/portal";
import { WebVNCInputTracker, webVNCInputBinding } from "../src/webvnc-input";

const leaseID = "cbx_000000000043";

describe("WebVNC input gate binding", () => {
  it("is stable per viewer session and lease and reveals neither", async () => {
    const binding = await webVNCInputBinding(leaseID, "session:webvnc_session_abc");
    expect(binding).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(await webVNCInputBinding(leaseID, "session:webvnc_session_abc")).toBe(binding);
    expect(await webVNCInputBinding(leaseID, "session:webvnc_session_other")).not.toBe(binding);
    expect(await webVNCInputBinding("cbx_000000000044", "session:webvnc_session_abc")).not.toBe(
      binding,
    );
    expect(binding).not.toContain("webvnc_session");
  });

  it("reserves input frames so a viewer cannot forge a binding or request", () => {
    for (const type of ["input_binding", "input_request", "input_state", "input_result"]) {
      expect(isReservedWebVNCControlFrame(JSON.stringify({ type }))).toBe(true);
    }
    expect(isReservedWebVNCControlFrame(JSON.stringify({ type: "keyboard" }))).toBe(false);
    expect(isReservedWebVNCControlFrame(' \n{"type":"input_binding"}')).toBe(true);
  });
});

describe("WebVNCInputTracker", () => {
  it("tracks each gate connection's state", () => {
    const tracker = new WebVNCInputTracker();
    expect(tracker.view(leaseID, "agent_1", false)).toEqual({ gate: false });
    expect(tracker.view(leaseID, "agent_1", true)).toEqual({
      gate: true,
      owner: "none",
      holder: "none",
    });
    expect(
      tracker.receive(
        leaseID,
        "agent_1",
        JSON.stringify({ type: "input_state", owner: "agent", holder: "none" }),
      ),
    ).toBe(true);
    expect(tracker.view(leaseID, "agent_1", true)).toEqual({
      gate: true,
      owner: "agent",
      holder: "none",
    });
    // Malformed states are consumed but ignored; other frames are not ours.
    expect(
      tracker.receive(leaseID, "agent_1", JSON.stringify({ type: "input_state", owner: "root" })),
    ).toBe(true);
    expect(tracker.view(leaseID, "agent_1", true).owner).toBe("agent");
    expect(tracker.receive(leaseID, "agent_1", "RFB 003.008\n")).toBe(false);
    expect(tracker.receive(leaseID, "agent_1", new ArrayBuffer(4))).toBe(false);
    tracker.forget(leaseID, "agent_1");
    expect(tracker.view(leaseID, "agent_1", true).owner).toBe("none");
  });

  it("answers a request from the same gate connection only, with coordinator text", async () => {
    const tracker = new WebVNCInputTracker();
    const sent: string[] = [];
    const socket = { send: (data: string) => sent.push(data) } as unknown as WebSocket;
    const pending = tracker.request(leaseID, "agent_1", socket, "take");
    const request = JSON.parse(sent[0]!) as { type: string; id: string; action: string };
    expect(request).toMatchObject({ type: "input_request", action: "take" });
    // Another connection cannot answer it.
    tracker.receive(
      leaseID,
      "agent_2",
      JSON.stringify({
        type: "input_result",
        id: request.id,
        ok: true,
        owner: "human",
        holder: "self",
      }),
    );
    tracker.receive(
      leaseID,
      "agent_1",
      JSON.stringify({
        type: "input_result",
        id: request.id,
        ok: false,
        code: "held_by_other",
        message: "<script>",
        owner: "human",
        holder: "other",
      }),
    );
    await expect(pending).resolves.toEqual({
      ok: false,
      owner: "human",
      holder: "other",
      code: "held_by_other",
      message: "Another viewer has control of this desktop.",
    });
  });

  it("fails closed when the connection closes or never answers", async () => {
    vi.useFakeTimers();
    try {
      const tracker = new WebVNCInputTracker();
      const socket = { send() {} } as unknown as WebSocket;
      const closed = tracker.request(leaseID, "agent_1", socket, "return");
      tracker.forget(leaseID, "agent_1");
      await expect(closed).resolves.toMatchObject({ ok: false, code: "bridge_disconnected" });
      const silent = tracker.request(leaseID, "agent_1", socket, "take");
      vi.advanceTimersByTime(45_000);
      await expect(silent).resolves.toMatchObject({ ok: false, code: "timeout" });
    } finally {
      vi.useRealTimers();
    }
  });
});

type Viewer = {
  id: string;
  agentID: string;
  socket: WebSocket;
  owner: string;
  label: string;
  viewerSessionID?: string;
};

function harness(capabilities: string[]) {
  const sent: string[] = [];
  const agentSocket = {
    readyState: 1,
    send: (data: string) => sent.push(data),
  } as unknown as WebSocket;
  const viewers: Viewer[] = [
    {
      id: "viewer_person",
      agentID: "agent_person",
      socket: { readyState: 1 } as WebSocket,
      owner: "stead@example.com",
      label: "person",
      viewerSessionID: "webvnc_session_person",
    },
    {
      id: "viewer_agent",
      agentID: "agent_agent",
      socket: { readyState: 1 } as WebSocket,
      owner: "stead@example.com",
      label: "agent",
      viewerSessionID: "webvnc_session_agent",
    },
  ];
  const fleet = Object.create(FleetCoordinator.prototype) as {
    webVNCInput: WebVNCInputTracker;
    webVNCInputChange(
      request: Request,
      identifier: string,
      session?: { session: string },
    ): Promise<Response>;
    webVNCInputView(leaseID: string, agentID: string | undefined): unknown;
  };
  Object.assign(fleet, {
    webVNCInput: new WebVNCInputTracker(),
    webVNCViewers: new Map([[leaseID, new Map(viewers.map((viewer) => [viewer.id, viewer]))]]),
    webVNCAgents: new Map([
      [
        leaseID,
        new Map([
          ["agent_person", agentSocket],
          ["agent_agent", agentSocket],
        ]),
      ],
    ]),
    webVNCAgentCapabilities: new Map([
      [
        leaseID,
        new Map([
          ["agent_person", new Set(capabilities)],
          ["agent_agent", new Set(capabilities)],
        ]),
      ],
    ]),
    webVNCEvents: new Map(),
    resolvePortalLease: async () => ({
      id: leaseID,
      state: "active",
      desktop: true,
      host: "127.0.0.1",
    }),
    currentBridgeRecipient: async (socket: WebSocket | undefined) => socket,
  });
  const change = (viewerID: string, action: string, session: string) =>
    fleet.webVNCInputChange(
      new Request("https://crabbox.test/portal/leases/x/vnc/input", {
        method: "POST",
        body: JSON.stringify({ viewerID, action }),
      }),
      leaseID,
      { session },
    );
  return { fleet, sent, change };
}

describe("POST /portal/leases/{lease}/vnc/input", () => {
  it("carries a take request down the requesting session's own viewer connection", async () => {
    const { fleet, sent, change } = harness(["desktop_theme", "input_gate"]);
    const response = change("viewer_person", "take", "webvnc_session_person");
    await new Promise((resolve) => setImmediate(resolve));
    const request = JSON.parse(sent[0]!) as { id: string; action: string };
    expect(request.action).toBe("take");
    fleet.webVNCInput.receive(
      leaseID,
      "agent_person",
      JSON.stringify({
        type: "input_result",
        id: request.id,
        ok: true,
        owner: "human",
        holder: "self",
      }),
    );
    const result = await response;
    expect(result.status).toBe(200);
    await expect(result.json()).resolves.toMatchObject({
      ok: true,
      owner: "human",
      holder: "self",
    });
    expect(fleet.webVNCInputView(leaseID, "agent_person")).toEqual({
      gate: true,
      owner: "human",
      holder: "self",
    });
  });

  it("refuses another session's viewer without contacting the gate", async () => {
    const { sent, change } = harness(["input_gate"]);
    const response = await change("viewer_person", "return", "webvnc_session_agent");
    expect(response.status).toBe(409);
    await expect(response.json()).resolves.toMatchObject({ error: "viewer_not_connected" });
    expect(sent).toEqual([]);
  });

  it("refuses when the desktop has no input gate", async () => {
    const { sent, change } = harness(["desktop_theme"]);
    const response = await change("viewer_person", "take", "webvnc_session_person");
    expect(response.status).toBe(409);
    await expect(response.json()).resolves.toMatchObject({ error: "input_gate_unavailable" });
    expect(sent).toEqual([]);
  });

  it("validates the request", async () => {
    const { change } = harness(["input_gate"]);
    const responses = await Promise.all([
      change("viewer_person", "grant", "webvnc_session_person"),
      change("nope", "take", "webvnc_session_person"),
    ]);
    expect(responses.map((response) => response.status)).toEqual([400, 400]);
  });
});

describe("viewer frames behind an input gate", () => {
  async function forwarded(capabilities: string[], messages: Array<string | ArrayBuffer>) {
    const sent: unknown[] = [];
    const agentSocket = {
      readyState: 1,
      send: (data: unknown) => sent.push(data),
    } as unknown as WebSocket;
    const viewerSocket = { readyState: 1 } as WebSocket;
    const fleet = Object.create(FleetCoordinator.prototype) as {
      handleBridgeMessage(
        socket: WebSocket,
        attachment: unknown,
        message: string | ArrayBuffer,
      ): Promise<void>;
    };
    Object.assign(fleet, {
      failedControlSockets: new Set(),
      bridgeSocketIsCurrent: () => true,
      restoredBridgesReady: async () => true,
      activeBridgeGrantIsCurrent: async () => true,
      currentBridgeRecipient: async (socket: WebSocket | undefined) => socket,
      webVNCAgents: new Map([[leaseID, new Map([["agent_1", agentSocket]])]]),
      webVNCAgentCapabilities: new Map([[leaseID, new Map([["agent_1", new Set(capabilities)]])]]),
    });
    const attachment = {
      kind: "webvnc-viewer",
      leaseID,
      id: "viewer_1",
      agentID: "agent_1",
      owner: "person",
    };
    for (const message of messages) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- a viewer's frames arrive in order.
      await fleet.handleBridgeMessage(viewerSocket, attachment, message);
    }
    return sent;
  }

  it("never lets a viewer's text frame reach a gate, whatever it looks like", async () => {
    const rfb = new Uint8Array([4, 1, 0, 0, 0, 0, 0, 97]).buffer;
    const forged = [
      '{"type":"input_binding","lease":"x","binding":"forged-binding-0000"}',
      ' {"type":"input_binding","lease":"x","binding":"forged-binding-0000"}',
      '{"TYPE":"input_request","id":"r1","action":"take"}',
      "plain text",
    ];
    const sent = await forwarded(["input_gate"], [...forged, rfb]);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toBeInstanceOf(ArrayBuffer);
    // Without a gate, ordinary text still passes as before.
    expect(await forwarded([], ["plain text"])).toEqual(["plain text"]);
  });
});

describe("WebVNC viewer page", () => {
  it("uses the input gate's state for take and return", async () => {
    const page = portalVNC({ id: leaseID, slug: "stead", target: "linux" } as never, {
      viewerOnly: true,
    });
    const body = await page.text();
    expect(body).toContain(`/portal/leases/${leaseID}/vnc/input`);
    expect(body).toContain("inputGate = state.input?.gate === true ? state.input : null;");
    expect(body).toContain('inputGate ? inputGate.holder === "self" : role === "controller"');
    expect(body).toContain('"return to agent"');
  });

  it("uses the embed viewer's own input route", async () => {
    const page = portalVNC({ id: leaseID, slug: "stead", target: "linux" } as never, {
      viewerOnly: true,
      embed: { frameAncestors: "https://bb.example", origin: "https://bb.example" },
    });
    const body = await page.text();
    expect(body).toContain(`/portal/leases/${leaseID}/vnc/embed/input`);
    expect(body).not.toContain(`/portal/leases/${leaseID}/vnc/input"`);
  });
});
