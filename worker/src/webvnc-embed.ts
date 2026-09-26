import type { Env } from "./types";

/**
 * The single browser origin allowed to frame the embeddable WebVNC viewer.
 *
 * `CRABBOX_WEBVNC_EMBED_ORIGIN` must be one exact `https://` origin (or
 * `http://localhost` for local development) with no path, query or fragment.
 * Anything else disables embed mode: tickets cannot be minted for it, its
 * routes answer as if they did not exist, and every embed page keeps
 * `frame-ancestors 'none'`.
 */
export function webVNCEmbedOrigin(
  env: Pick<Env, "CRABBOX_WEBVNC_EMBED_ORIGIN">,
): string | undefined {
  const raw = env.CRABBOX_WEBVNC_EMBED_ORIGIN?.trim();
  if (!raw) return undefined;
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return undefined;
  }
  if (url.username || url.password || url.search || url.hash) return undefined;
  if (url.pathname !== "/" || raw.endsWith("/")) return undefined;
  if (!/^[a-z0-9.-]+$/i.test(url.hostname)) return undefined;
  const localhost = url.hostname === "localhost" || url.hostname === "127.0.0.1";
  if (url.protocol !== "https:" && !(url.protocol === "http:" && localhost)) return undefined;
  if (url.origin === "null" || url.origin !== raw) return undefined;
  return url.origin;
}

/** The `frame-ancestors` source list for embed responses. */
export function webVNCEmbedFrameAncestors(env: Pick<Env, "CRABBOX_WEBVNC_EMBED_ORIGIN">): string {
  return webVNCEmbedOrigin(env) ?? "'none'";
}

export const webVNCEmbedMessageType = "crabbox-webvnc-embed";
