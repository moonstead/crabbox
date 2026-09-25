const runtimeAdapterIDPattern = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

export const runtimeAdapterRelayBodyLimit = 64 * 1024;
// Covers a maximally JSON-escaped body plus the bounded response envelope.
export const runtimeAdapterRelayFrameLimit = 512 * 1024;
// The connector allows ordinary local calls 9s, then needs up to 5s to write
// the response back over the relay WebSocket.
export const runtimeAdapterRelayTimeoutMs = 14_000;
export const runtimeAdapterDesktopRelayTimeoutMs = 150_000;
export const runtimeAdapterDesktopRelayMaxTimeoutMs = 24 * 60 * 60 * 1_000 + 35_000;
// Workspace exec is relayed only to connectors that advertise an exec budget:
// the command timeout plus 30s of setup, plus 5s of response delivery.
export const runtimeAdapterExecBodyLimit = 256 * 1024;
export const runtimeAdapterExecRelayOverheadMs = 35_000;
export const runtimeAdapterExecRelayMaxTimeoutMs =
  60 * 60 * 1_000 + runtimeAdapterExecRelayOverheadMs;
export const runtimeAdapterExecMinTimeoutMs = 1_000;
export const runtimeAdapterExecMaxTimeoutMs = 60 * 60 * 1_000;
export const runtimeAdapterExecMaxArgs = 256;
export const runtimeAdapterExecMaxArgvBytes = 32 * 1024;
export const runtimeAdapterExecMaxPendingPerAdapter = 4;

export type RuntimeAdapterRelayRequest = {
  type: "request";
  id: string;
  method: "GET" | "POST" | "DELETE";
  path: string;
  /** Absolute Unix epoch deadline. The connector must not start expired work. */
  deadlineMs: number;
  headers?: Record<string, string>;
  body?: string;
};

/** Withdraws an exec request whose caller went away or whose deadline passed. */
export type RuntimeAdapterRelayCancel = {
  type: "cancel";
  id: string;
};

export type RuntimeAdapterExecRequest = {
  argv: string[];
  stdinBase64?: string;
  timeoutMs: number;
  leaseId?: string;
  registrationId?: string;
};

export type RuntimeAdapterRelayResponse = {
  type: "response";
  id: string;
  status: number;
  headers?: Record<string, string>;
  body?: string;
};

export function validRuntimeAdapterID(value: unknown): value is string {
  return typeof value === "string" && runtimeAdapterIDPattern.test(value);
}

export function runtimeAdapterProxyPath(parts: string[]): string | undefined {
  if (parts[0] !== "v1" || parts[1] !== "workspaces") {
    return undefined;
  }
  if (parts.length === 2) {
    return "/v1/workspaces";
  }
  if (!validRuntimeAdapterID(parts[2])) {
    return undefined;
  }
  if (parts.length === 3) {
    return `/v1/workspaces/${parts[2]}`;
  }
  if (
    parts.length === 5 &&
    parts[3] === "connections" &&
    (parts[4] === "desktop" || parts[4] === "native-vnc")
  ) {
    return `/v1/workspaces/${parts[2]}/connections/${parts[4]}`;
  }
  return undefined;
}

/**
 * Returns the adapter path for POST /v1/workspaces/{id}/exec. It is separate
 * from runtimeAdapterProxyPath so service authentication and the ordinary
 * lifecycle relay surface never include command execution.
 */
export function runtimeAdapterExecPath(parts: string[]): string | undefined {
  if (
    parts.length === 4 &&
    parts[0] === "v1" &&
    parts[1] === "workspaces" &&
    validRuntimeAdapterID(parts[2]) &&
    parts[3] === "exec"
  ) {
    return `/v1/workspaces/${parts[2]}/exec`;
  }
  return undefined;
}

export function validRuntimeAdapterExecRelayTimeout(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= runtimeAdapterRelayTimeoutMs &&
    value <= runtimeAdapterExecRelayMaxTimeoutMs
  );
}

const runtimeAdapterExecKeys = new Set([
  "argv",
  "stdinBase64",
  "timeoutMs",
  "leaseId",
  "registrationId",
]);
const standardBase64Pattern = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const canonicalLeaseIDPattern = /^cbx_[a-f0-9]{12}$/;

/**
 * Parses a caller's exec request strictly. It never returns or quotes the
 * body: stdin is private and must not reach errors or logs.
 */
export function parseRuntimeAdapterExecRequest(
  body: string,
): RuntimeAdapterExecRequest | undefined {
  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    return undefined;
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const input = value as Record<string, unknown>;
  if (Object.keys(input).some((key) => !runtimeAdapterExecKeys.has(key))) return undefined;
  const { argv, stdinBase64, timeoutMs, leaseId, registrationId } = input;
  if (
    !Array.isArray(argv) ||
    argv.length < 1 ||
    argv.length > runtimeAdapterExecMaxArgs ||
    argv.some((arg) => typeof arg !== "string" || arg.includes("\0")) ||
    argv[0] === ""
  ) {
    return undefined;
  }
  const encoder = new TextEncoder();
  const argvBytes = (argv as string[]).reduce(
    (total, arg) => total + encoder.encode(arg).byteLength,
    0,
  );
  if (argvBytes > runtimeAdapterExecMaxArgvBytes) return undefined;
  if (
    stdinBase64 !== undefined &&
    (typeof stdinBase64 !== "string" || !standardBase64Pattern.test(stdinBase64))
  ) {
    return undefined;
  }
  if (
    typeof timeoutMs !== "number" ||
    !Number.isSafeInteger(timeoutMs) ||
    timeoutMs < runtimeAdapterExecMinTimeoutMs ||
    timeoutMs > runtimeAdapterExecMaxTimeoutMs
  ) {
    return undefined;
  }
  if (
    leaseId !== undefined &&
    (typeof leaseId !== "string" || !canonicalLeaseIDPattern.test(leaseId))
  ) {
    return undefined;
  }
  if (registrationId !== undefined && !validRuntimeAdapterID(registrationId)) {
    return undefined;
  }
  return {
    argv: argv as string[],
    ...(stdinBase64 === undefined || stdinBase64 === "" ? {} : { stdinBase64 }),
    timeoutMs,
    ...(leaseId === undefined ? {} : { leaseId }),
    ...(registrationId === undefined ? {} : { registrationId }),
  };
}

/** Binds an admitted request to the coordinator's exact lease generation. */
export function runtimeAdapterExecRelayBody(
  request: RuntimeAdapterExecRequest,
  leaseId: string,
  registrationId: string,
): string {
  return JSON.stringify({
    argv: request.argv,
    ...(request.stdinBase64 === undefined ? {} : { stdinBase64: request.stdinBase64 }),
    timeoutMs: request.timeoutMs,
    leaseId,
    registrationId,
  });
}

export function runtimeAdapterRelayMethodAllowed(method: string, path: string): boolean {
  if (path === "/v1/workspaces") {
    return method === "POST";
  }
  if (path.endsWith("/connections/desktop") || path.endsWith("/connections/native-vnc")) {
    return method === "POST";
  }
  return method === "GET" || method === "DELETE";
}

export function runtimeAdapterRelayBodyAllowed(
  method: string,
  path: string,
  body: string | undefined,
): boolean {
  return body === undefined || body === "" || (method === "POST" && path === "/v1/workspaces");
}

export function validRuntimeAdapterDesktopRelayTimeout(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= runtimeAdapterRelayTimeoutMs &&
    value <= runtimeAdapterDesktopRelayMaxTimeoutMs
  );
}

export function runtimeAdapterRelayTimeoutForPath(path: string, desktopTimeoutMs?: number): number {
  if (!path.endsWith("/connections/desktop") && !path.endsWith("/connections/native-vnc")) {
    return runtimeAdapterRelayTimeoutMs;
  }
  return validRuntimeAdapterDesktopRelayTimeout(desktopTimeoutMs)
    ? desktopTimeoutMs
    : runtimeAdapterDesktopRelayTimeoutMs;
}

export function runtimeAdapterRelayHeaders(request: Request): Record<string, string> | undefined {
  const idempotencyKey = request.headers.get("idempotency-key")?.trim();
  if (!idempotencyKey) return undefined;
  if (idempotencyKey.length > 128) {
    throw new RangeError("runtime adapter idempotency key is too long");
  }
  return { "idempotency-key": idempotencyKey };
}

export function runtimeAdapterRelayContentType(
  headers: Record<string, string> | undefined,
): string | undefined {
  return Object.entries(headers ?? {}).find(([key]) => key.toLowerCase() === "content-type")?.[1];
}

export async function readRuntimeAdapterRelayBody(
  request: Request,
  limit = runtimeAdapterRelayBodyLimit,
): Promise<string | undefined> {
  if (!request.body) {
    return undefined;
  }
  const declared = Number(request.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > limit) {
    throw new RangeError("runtime adapter request body is too large");
  }
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  while (true) {
    // eslint-disable-next-line no-await-in-loop -- a bounded stream must be consumed in order.
    const { done, value } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > limit) {
      void reader.cancel();
      throw new RangeError("runtime adapter request body is too large");
    }
    chunks.push(value);
  }
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(body);
  } catch {
    throw new TypeError("runtime adapter request body must be valid UTF-8");
  }
}

export function validRuntimeAdapterRelayResponse(
  value: unknown,
  expectedID: string,
): value is RuntimeAdapterRelayResponse {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const response = value as Partial<RuntimeAdapterRelayResponse>;
  const headers = response.headers;
  if (
    headers !== undefined &&
    (!headers ||
      typeof headers !== "object" ||
      Array.isArray(headers) ||
      Object.entries(headers).some(
        ([key, header]) =>
          key.toLowerCase() !== "content-type" ||
          typeof header !== "string" ||
          /[\r\n]/.test(header) ||
          new TextEncoder().encode(header).byteLength > 256,
      ))
  ) {
    return false;
  }
  return Boolean(
    response.type === "response" &&
    response.id === expectedID &&
    Number.isInteger(response.status) &&
    (response.status ?? 0) >= 200 &&
    (response.status ?? 0) <= 599 &&
    (response.body === undefined ||
      (typeof response.body === "string" &&
        new TextEncoder().encode(response.body).byteLength <= runtimeAdapterRelayBodyLimit)),
  );
}
