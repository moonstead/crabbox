import { describe, expect, it } from "vitest";

import { webVNCEmbedFrameAncestors, webVNCEmbedOrigin } from "../src/webvnc-embed";

describe("webVNCEmbedOrigin", () => {
  it("accepts exactly one https origin", () => {
    expect(webVNCEmbedOrigin({ CRABBOX_WEBVNC_EMBED_ORIGIN: "https://bb.example.test" })).toBe(
      "https://bb.example.test",
    );
    expect(
      webVNCEmbedOrigin({ CRABBOX_WEBVNC_EMBED_ORIGIN: "  https://bb.example.test:8443  " }),
    ).toBe("https://bb.example.test:8443");
    expect(webVNCEmbedOrigin({ CRABBOX_WEBVNC_EMBED_ORIGIN: "http://localhost:3000" })).toBe(
      "http://localhost:3000",
    );
    expect(
      webVNCEmbedFrameAncestors({ CRABBOX_WEBVNC_EMBED_ORIGIN: "https://bb.example.test" }),
    ).toBe("https://bb.example.test");
  });

  it("disables embed mode for anything that is not one exact origin", () => {
    for (const value of [
      undefined,
      "",
      "   ",
      "bb.example.test",
      "https://bb.example.test/",
      "https://bb.example.test/panel",
      "https://bb.example.test?x=1",
      "https://bb.example.test#x",
      "https://user:pw@bb.example.test",
      "http://bb.example.test",
      "https://bb.example.test https://other.example.test",
      "https://*.example.test",
      "'self'",
      "*",
      "null",
      "file:///tmp/panel.html",
      "javascript:alert(1)",
    ]) {
      expect({ value, origin: webVNCEmbedOrigin({ CRABBOX_WEBVNC_EMBED_ORIGIN: value }) }).toEqual({
        value,
        origin: undefined,
      });
      expect({
        value,
        ancestors: webVNCEmbedFrameAncestors({ CRABBOX_WEBVNC_EMBED_ORIGIN: value }),
      }).toEqual({ value, ancestors: "'none'" });
    }
  });
});
