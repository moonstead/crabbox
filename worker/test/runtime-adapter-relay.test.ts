import { describe, expect, it } from "vitest";

import {
  parseRuntimeAdapterExecRequest,
  readRuntimeAdapterRelayBody,
  runtimeAdapterExecBodyLimit,
  runtimeAdapterExecPath,
  runtimeAdapterExecRelayBody,
  runtimeAdapterExecRelayMaxTimeoutMs,
  runtimeAdapterProxyPath,
  runtimeAdapterRelayBodyAllowed,
  runtimeAdapterRelayBodyLimit,
  runtimeAdapterRelayContentType,
  runtimeAdapterDesktopRelayTimeoutMs,
  runtimeAdapterRelayFrameLimit,
  runtimeAdapterRelayHeaders,
  runtimeAdapterRelayMethodAllowed,
  runtimeAdapterRelayTimeoutForPath,
  runtimeAdapterRelayTimeoutMs,
  validRuntimeAdapterExecRelayTimeout,
  validRuntimeAdapterID,
  validRuntimeAdapterRelayResponse,
} from "../src/runtime-adapter-relay";

describe("runtime adapter relay", () => {
  it("bounds a fully escaped response envelope", () => {
    expect(runtimeAdapterRelayFrameLimit).toBeGreaterThan(runtimeAdapterRelayBodyLimit * 6);
  });

  it("accepts only stable DNS-style adapter and workspace IDs", () => {
    expect(validRuntimeAdapterID("example-adapter")).toBe(true);
    expect(validRuntimeAdapterID("a".repeat(63))).toBe(true);
    expect(validRuntimeAdapterID("Example-Adapter")).toBe(false);
    expect(validRuntimeAdapterID("a".repeat(64))).toBe(false);
    expect(validRuntimeAdapterID("../workspace")).toBe(false);
  });

  it("allows only the versioned lifecycle routes and methods", () => {
    expect(runtimeAdapterProxyPath(["v1", "workspaces"])).toBe("/v1/workspaces");
    expect(runtimeAdapterProxyPath(["v1", "workspaces", "example-workspace"])).toBe(
      "/v1/workspaces/example-workspace",
    );
    expect(
      runtimeAdapterProxyPath(["v1", "workspaces", "example-workspace", "connections", "desktop"]),
    ).toBe("/v1/workspaces/example-workspace/connections/desktop");
    expect(
      runtimeAdapterProxyPath([
        "v1",
        "workspaces",
        "example-workspace",
        "connections",
        "native-vnc",
      ]),
    ).toBe("/v1/workspaces/example-workspace/connections/native-vnc");
    expect(
      runtimeAdapterProxyPath(["v1", "workspaces", "example-workspace", "shell"]),
    ).toBeUndefined();
    expect(runtimeAdapterProxyPath(["v1", "workspaces", ".."])).toBeUndefined();

    expect(runtimeAdapterRelayMethodAllowed("POST", "/v1/workspaces")).toBe(true);
    expect(runtimeAdapterRelayMethodAllowed("DELETE", "/v1/workspaces")).toBe(false);
    expect(runtimeAdapterRelayMethodAllowed("GET", "/v1/workspaces/example-workspace")).toBe(true);
    expect(runtimeAdapterRelayMethodAllowed("DELETE", "/v1/workspaces/example-workspace")).toBe(
      true,
    );
    expect(
      runtimeAdapterRelayMethodAllowed(
        "POST",
        "/v1/workspaces/example-workspace/connections/desktop",
      ),
    ).toBe(true);
    expect(
      runtimeAdapterRelayMethodAllowed(
        "POST",
        "/v1/workspaces/example-workspace/connections/native-vnc",
      ),
    ).toBe(true);
    expect(runtimeAdapterRelayBodyAllowed("POST", "/v1/workspaces", "{}")).toBe(true);
    expect(
      runtimeAdapterRelayBodyAllowed(
        "POST",
        "/v1/workspaces/example-workspace/connections/desktop",
        undefined,
      ),
    ).toBe(true);
    expect(
      runtimeAdapterRelayBodyAllowed(
        "POST",
        "/v1/workspaces/example-workspace/connections/native-vnc",
        undefined,
      ),
    ).toBe(true);
    expect(
      runtimeAdapterRelayBodyAllowed(
        "POST",
        "/v1/workspaces/example-workspace/connections/desktop",
        "{}",
      ),
    ).toBe(false);
    expect(runtimeAdapterRelayTimeoutForPath("/v1/workspaces/example-workspace")).toBe(
      runtimeAdapterRelayTimeoutMs,
    );
    expect(
      runtimeAdapterRelayTimeoutForPath("/v1/workspaces/example-workspace/connections/desktop"),
    ).toBe(runtimeAdapterDesktopRelayTimeoutMs);
    expect(
      runtimeAdapterRelayTimeoutForPath("/v1/workspaces/example-workspace/connections/native-vnc"),
    ).toBe(runtimeAdapterDesktopRelayTimeoutMs);
    expect(
      runtimeAdapterRelayTimeoutForPath(
        "/v1/workspaces/example-workspace/connections/desktop",
        11 * 60 * 1_000,
      ),
    ).toBe(11 * 60 * 1_000);
    expect(
      runtimeAdapterRelayTimeoutForPath(
        "/v1/workspaces/example-workspace/connections/native-vnc",
        11 * 60 * 1_000,
      ),
    ).toBe(11 * 60 * 1_000);
  });

  it("forwards only a bounded idempotency key", () => {
    expect(
      runtimeAdapterRelayHeaders(
        new Request("https://example.test", {
          headers: { "idempotency-key": "example-workspace", authorization: "Bearer secret" },
        }),
      ),
    ).toEqual({ "idempotency-key": "example-workspace" });
    expect(() =>
      runtimeAdapterRelayHeaders(
        new Request("https://example.test", {
          headers: { "idempotency-key": "x".repeat(129) },
        }),
      ),
    ).toThrow("runtime adapter idempotency key is too long");
  });

  it("reads response content type case-insensitively", () => {
    expect(runtimeAdapterRelayContentType({ "Content-Type": "application/json" })).toBe(
      "application/json",
    );
    expect(runtimeAdapterRelayContentType({ "content-TYPE": "text/plain" })).toBe("text/plain");
    expect(runtimeAdapterRelayContentType(undefined)).toBeUndefined();
  });

  it("bounds streamed request and response bodies", async () => {
    await expect(
      readRuntimeAdapterRelayBody(
        new Request("https://example.test", { method: "POST", body: '{"id":"example-workspace"}' }),
      ),
    ).resolves.toBe('{"id":"example-workspace"}');
    await expect(
      readRuntimeAdapterRelayBody(
        new Request("https://example.test", {
          method: "POST",
          body: "x".repeat(runtimeAdapterRelayBodyLimit + 1),
        }),
      ),
    ).rejects.toThrow("too large");
    await expect(
      readRuntimeAdapterRelayBody(
        new Request("https://example.test", {
          method: "POST",
          body: new Uint8Array([0xc3, 0x28]),
        }),
      ),
    ).rejects.toThrow("must be valid UTF-8");

    expect(
      validRuntimeAdapterRelayResponse(
        { type: "response", id: "request-1", status: 202, body: '{"status":"stopping"}' },
        "request-1",
      ),
    ).toBe(true);
    expect(
      validRuntimeAdapterRelayResponse(
        { type: "response", id: "request-1", status: 200, body: "x".repeat(65_537) },
        "request-1",
      ),
    ).toBe(false);
    expect(
      validRuntimeAdapterRelayResponse(
        {
          type: "response",
          id: "request-1",
          status: 200,
          headers: { authorization: "secret" },
        },
        "request-1",
      ),
    ).toBe(false);
  });

  it("keeps exec off the ordinary proxy and service-auth surface", () => {
    const parts = ["v1", "workspaces", "example-workspace", "exec"];
    expect(runtimeAdapterProxyPath(parts)).toBeUndefined();
    expect(runtimeAdapterExecPath(parts)).toBe("/v1/workspaces/example-workspace/exec");
    expect(runtimeAdapterExecPath(["v1", "workspaces", "Example", "exec"])).toBeUndefined();
    expect(runtimeAdapterExecPath(["v1", "workspaces", "..", "exec"])).toBeUndefined();
    expect(
      runtimeAdapterExecPath(["v1", "workspaces", "example-workspace", "exec", "x"]),
    ).toBeUndefined();
    expect(runtimeAdapterExecPath(["v1", "workspaces", "exec"])).toBeUndefined();
  });

  it("parses exec requests strictly and binds the coordinator generation", () => {
    const secret = "enrolment-secret-5f2c9e";
    const valid = {
      argv: ["sh", "-s", "--", "--start"],
      stdinBase64: btoa(secret),
      timeoutMs: 600_000,
    };
    const parsed = parseRuntimeAdapterExecRequest(JSON.stringify(valid));
    expect(parsed).toEqual(valid);
    expect(
      JSON.parse(runtimeAdapterExecRelayBody(parsed!, "cbx_000000000007", "registration-7")),
    ).toEqual({
      ...valid,
      leaseId: "cbx_000000000007",
      registrationId: "registration-7",
    });
    for (const body of [
      "not json",
      "[]",
      JSON.stringify({ ...valid, env: { TOKEN: secret } }),
      JSON.stringify({ ...valid, argv: [] }),
      JSON.stringify({ ...valid, argv: [""] }),
      JSON.stringify({ ...valid, argv: ["sh", 1] }),
      JSON.stringify({ ...valid, argv: ["sh", "a\0b"] }),
      JSON.stringify({ ...valid, argv: ["sh", "x".repeat(32 * 1024)] }),
      JSON.stringify({ ...valid, argv: Array.from({ length: 257 }, () => "a") }),
      JSON.stringify({ ...valid, stdinBase64: "not base64!" }),
      JSON.stringify({ ...valid, timeoutMs: 999 }),
      JSON.stringify({ ...valid, timeoutMs: 60 * 60 * 1000 + 1 }),
      JSON.stringify({ ...valid, timeoutMs: 1.5 }),
      JSON.stringify({ ...valid, leaseId: "cbx_ZZZ" }),
      JSON.stringify({ ...valid, registrationId: "Registration_7" }),
    ]) {
      expect(parseRuntimeAdapterExecRequest(body)).toBeUndefined();
    }
  });

  it("bounds exec bodies and connector exec budgets", async () => {
    expect(runtimeAdapterExecBodyLimit).toBe(256 * 1024);
    const oversized = new Request("https://crabbox.test/exec", {
      method: "POST",
      body: "x".repeat(runtimeAdapterExecBodyLimit + 1),
    });
    await expect(
      readRuntimeAdapterRelayBody(oversized, runtimeAdapterExecBodyLimit),
    ).rejects.toThrow(RangeError);
    const ordinary = new Request("https://crabbox.test/exec", {
      method: "POST",
      body: "x".repeat(runtimeAdapterRelayBodyLimit + 1),
    });
    await expect(readRuntimeAdapterRelayBody(ordinary)).rejects.toThrow(RangeError);
    expect(validRuntimeAdapterExecRelayTimeout(runtimeAdapterRelayTimeoutMs)).toBe(true);
    expect(validRuntimeAdapterExecRelayTimeout(runtimeAdapterExecRelayMaxTimeoutMs)).toBe(true);
    expect(validRuntimeAdapterExecRelayTimeout(runtimeAdapterExecRelayMaxTimeoutMs + 1)).toBe(
      false,
    );
    expect(validRuntimeAdapterExecRelayTimeout(1.5)).toBe(false);
  });
});
