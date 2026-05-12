// ─── 客户端续流网关 (Client Stream Gateway / CSG) ────────────────────────────
//
// MUST NOT run on Replit Autoscale or any PaaS with hard inbound connection
// time limits. The client-facing connection is a long-lived HTTP/SSE stream;
// running on Autoscale would re-create the same 300s GFE wall we are trying
// to bypass. Run this on a local machine, NAS, or VPS instead.
//
// URL contract (inbound from client):
//   https://<csg-host>/<node-host>/<original-path>?<query>
//   e.g. https://csg.local:3000/foo-node.replit.app/v1/chat/completions
//
//   First path segment  = node hostname (may include port)
//   Rest of path + qs   = forwarded verbatim to the node as /csg/<original-path>?<query>
//
// The CSG:
//   1. POSTs /csg/<original-path> to the node  → {sessionId, ttlMs}
//   2. Loops: POST /csg/pull/<id> {since, ack, maxWaitMs:240000}
//      - First response with `first` → res.writeHead(status, headers)
//      - Each chunk decoded from base64 → res.write(raw bytes)
//      - done=true → res.end()
//   3. On client disconnect → POST /csg/cancel/<id>  (fire-and-forget)
//
// Auth: the client's Authorization header is passed through unchanged to the
// node. CSG itself never inspects or validates it.
//
// Special endpoints (handled directly by CSG, NOT forwarded):
//   GET     /healthz   → 200 {"ok":true}
//   OPTIONS *          → 204 with permissive CORS preflight headers
//
// CORS: every response (success, error, preflight) includes
// `Access-Control-Allow-Origin: *` and friends so browser-based clients can
// read responses cross-origin. Without this, OPTIONS preflights would be
// forwarded to the node's /csg/<path>, which only accepts POST, returning
// empty bodies that CSG cannot parse.

import http from "node:http";
import process from "node:process";
import crypto from "node:crypto";

// Trace 头名（与 Node 端 lib/traceContext.ts TRACE_HEADER 一致）。
const TRACE_HEADER = "x-csg-trace-id";

// 是否回传 CSG 侧 trace recap 到 Node 的 /api/csg/trace/recap。
// 默认关闭：开启需要 CSG 也持有 Node 的 Proxy Key（运维独立部署场景下未必有）。
const REPORT_TRACES = process.env.CSG_REPORT_TRACES === "1";
const REPORT_PROXY_KEY = process.env.CSG_PROXY_KEY ?? "";

function newTraceId(): string {
  return crypto.randomBytes(16).toString("hex");
}

// ─── Constants ───────────────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT ?? "3000", 10);

// Protocol used when reaching the target node. Default is https (correct for
// real *.replit.app nodes). Override to "http" only for local dev/testing:
//   NODE_PROTOCOL=http node dist/index.js
// Never set this to "http" in production — you'd send the client's API key
// in plaintext over the network.
const NODE_PROTOCOL = (process.env.NODE_PROTOCOL ?? "https").replace(/:?\/*$/, "");

// Long-poll window forwarded to each /csg/pull call. Must stay well under
// the GFE 300s wall (60s safety margin). Do NOT make this configurable —
// it is a protocol constant, not a tunable preference.
const MAX_WAIT_MS = 240_000;

// Hop-by-hop / transport headers that must not be forwarded between hops.
// `host` is replaced by the target node's host. `content-length` is
// re-computed by the Node.js fetch implementation from the actual body bytes.
const HOP_BY_HOP = new Set([
  "host",
  "content-length",
  "connection",
  "keep-alive",
  "transfer-encoding",
  "upgrade",
  "te",
  "trailer",
  "proxy-authenticate",
  "proxy-authorization",
  "expect",
]);

// ─── Types (mirrors pullSession.ts / csg.ts PollResult) ──────────────────────

interface PollResult {
  first?: { status: number; headers: Record<string, string> };
  chunks: Array<{ seq: number; dataB64: string }>;
  done: boolean;
  error?: { message: string; phase?: string } | null;
  nextSeq: number;
}

// CORS headers attached to every response. `*` is correct for an API gateway
// — clients authenticate per-request via the Authorization header, not via
// browser-managed credentials, so we never need to echo the Origin or set
// `Allow-Credentials: true`.
const CORS_HEADERS: Record<string, string> = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS",
  "access-control-allow-headers": "*",
  "access-control-expose-headers": "*",
  "access-control-max-age": "86400",
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

function log(level: "INFO" | "WARN" | "ERROR", msg: string, extra?: object): void {
  const line = JSON.stringify({ time: new Date().toISOString(), level, msg, ...extra });
  if (level === "ERROR") process.stderr.write(line + "\n");
  else process.stdout.write(line + "\n");
}

// Per-request trace event collector for CSG-side recap.
interface CsgTraceEvent { t: number; level: string; msg: string; [k: string]: unknown }
function makeTraceCollector(traceId: string) {
  const events: CsgTraceEvent[] = [];
  function emit(level: "INFO" | "WARN" | "ERROR", msg: string, extra?: object): void {
    const ev: CsgTraceEvent = { t: Date.now(), level, msg, traceId, ...(extra ?? {}) };
    events.push(ev);
    log(level, msg, { traceId, ...(extra ?? {}) });
  }
  return { events, emit };
}

/** Collect all chunks from the incoming request into a single Buffer. */
async function readBody(req: http.IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of req as AsyncIterable<Buffer | string>) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

/** Filter client headers down to what is safe to forward to the node. */
function buildForwardHeaders(
  incoming: http.IncomingHttpHeaders,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(incoming)) {
    if (v === undefined) continue;
    const lower = k.toLowerCase();
    if (HOP_BY_HOP.has(lower)) continue;
    out[lower] = Array.isArray(v) ? v.join(", ") : String(v);
  }
  return out;
}

/** Send a JSON error and end the response (only if headers not yet sent). */
function sendError(
  res: http.ServerResponse,
  status: number,
  type: string,
  message: string,
): void {
  if (res.headersSent) {
    // Headers already sent (stream started) — destroy so the client's
    // SDK detects a truncated stream rather than a clean 200 finish.
    res.destroy();
    return;
  }
  try {
    res.writeHead(status, { ...CORS_HEADERS, "content-type": "application/json" });
    res.end(JSON.stringify({ error: { type, message } }));
  } catch {
    res.destroy();
  }
}

// ─── Request handler ─────────────────────────────────────────────────────────

async function handleRequest(
  req: http.IncomingMessage,
  res: http.ServerResponse,
): Promise<void> {
  const rawUrl = req.url ?? "/";
  const method = (req.method ?? "GET").toUpperCase();

  // ── CORS preflight ─────────────────────────────────────────────────────────
  // Browser clients send OPTIONS before any cross-origin POST. The node's
  // /csg/<path> only accepts POST, so forwarding OPTIONS would 4xx with an
  // empty body and surface as `error: {}` to the client. Short-circuit here.
  if (method === "OPTIONS") {
    res.writeHead(204, CORS_HEADERS);
    res.end();
    return;
  }

  // ── Healthcheck ────────────────────────────────────────────────────────────
  if (rawUrl === "/healthz" || rawUrl === "/health") {
    res.writeHead(200, { ...CORS_HEADERS, "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  // ── Parse /<nodeHost>/<originalPath>?<query> ───────────────────────────────
  // Strip leading slash, split on first remaining slash.
  const withoutLeading = rawUrl.startsWith("/") ? rawUrl.slice(1) : rawUrl;
  const firstSlash = withoutLeading.indexOf("/");
  if (firstSlash === -1) {
    sendError(res, 400, "invalid_url", "URL must be /<nodeHost>/<path>[?query]");
    return;
  }
  const nodeHost = withoutLeading.slice(0, firstSlash);
  // Everything after the node host, including query string.
  const originalPath = withoutLeading.slice(firstSlash); // has leading /

  if (!nodeHost) {
    sendError(res, 400, "invalid_url", "Empty node host in URL");
    return;
  }

  const nodeBase = `${NODE_PROTOCOL}://${nodeHost}`;
  const forwardHeaders = buildForwardHeaders(req.headers);

  // 端到端 traceId：客户端可显式传 x-csg-trace-id；否则我们生成。每次 fetch 都
  // 带上同一个 traceId，Node 端 traceLog 会把所有 hop 串到一条 trace。
  const incomingTrace = forwardHeaders[TRACE_HEADER];
  const traceId = (typeof incomingTrace === "string" && /^[a-zA-Z0-9_-]{8,128}$/.test(incomingTrace.trim()))
    ? incomingTrace.trim()
    : newTraceId();
  forwardHeaders[TRACE_HEADER] = traceId;

  // Capture auth header for cancel / pull calls (doesn't need body parsing).
  const authHeader = forwardHeaders["authorization"] ?? "";

  const tracer = makeTraceCollector(traceId);

  // ── Read client body ───────────────────────────────────────────────────────
  const BODY_METHODS = new Set(["POST", "PUT", "PATCH"]);
  let body: Buffer | undefined;
  if (BODY_METHODS.has(method)) {
    try {
      const buf = await readBody(req);
      body = buf.length > 0 ? buf : undefined;
    } catch (err) {
      sendError(res, 400, "body_read_error", String((err as Error)?.message ?? err));
      return;
    }
  }

  // ── Session lifecycle ──────────────────────────────────────────────────────
  let sessionId: string | null = null;
  let cancelled = false;

  // Per-request observability state. Every log line carries these so a single
  // session's lifetime can be reconstructed by grepping `sessionId`.
  const reqStartedAt = Date.now();
  const elapsed = (): number => Date.now() - reqStartedAt;
  let pollCount = 0;
  let totalChunksReceived = 0;
  let totalBytesReceived = 0;
  let totalBytesWritten = 0;     // bytes successfully res.write()'n to client
  let lastWriteAt = reqStartedAt; // wall-clock of last successful client write
  let lastChunkSeqWritten = -1;
  let upstreamStatus: number | null = null;
  let upstreamContentType: string | null = null;
  // Cause-of-death: filled in by whichever code path terminates the request.
  // Logged on the final `request finished` line so we can always see exactly
  // why the stream ended (client closed, upstream done, error, etc).
  let endCause: string = "unknown";

  function cancelSession(reason: string): void {
    if (!sessionId || cancelled) return;
    const sid = sessionId;
    sessionId = null;
    cancelled = true;
    log("INFO", "cancel sent", {
      sessionId: sid, reason,
      elapsedMs: elapsed(),
      pollCount, totalBytesReceived, totalBytesWritten,
      lastChunkSeqWritten,
    });
    // Fire-and-forget. Errors are expected (e.g. node unreachable after
    // the session was already consumed) — swallow them silently.
    fetch(`${nodeBase}/csg/cancel/${sid}`, {
      method: "POST",
      headers: { authorization: authHeader, [TRACE_HEADER]: traceId },
    }).catch(() => {});
  }

  // Cancel when the client disconnects before the stream finishes. This is
  // THE critical signal for diagnosing 300s-mid-stream truncations: when the
  // client (or anything between client and CSG) closes the TCP connection,
  // Node fires `close` on the IncomingMessage. The elapsed-ms here tells us
  // exactly how long the client kept the socket open before giving up.
  req.on("close", () => {
    if (!res.writableEnded) {
      log("WARN", "client connection closed mid-stream", {
        sessionId,
        elapsedMs: elapsed(),
        msSinceLastWrite: Date.now() - lastWriteAt,
        pollCount,
        totalBytesReceived,
        totalBytesWritten,
        lastChunkSeqWritten,
        upstreamStatus,
        upstreamContentType,
        // res.writableEnded is false here by definition; surface other state
        // so we can tell apart "client RST during active write" from "client
        // RST during silent poll wait".
        resDestroyed: res.destroyed,
        socketDestroyed: req.socket?.destroyed ?? null,
      });
      endCause = endCause === "unknown" ? "client_closed" : endCause;
      cancelSession("client_disconnected");
    }
  });
  req.on("error", (err) => {
    log("WARN", "client request error", {
      sessionId, elapsedMs: elapsed(), err: String(err),
      code: (err as NodeJS.ErrnoException)?.code ?? null,
    });
  });
  res.on("error", (err) => {
    log("WARN", "client response error", {
      sessionId, elapsedMs: elapsed(), err: String(err),
      code: (err as NodeJS.ErrnoException)?.code ?? null,
      totalBytesWritten,
    });
  });
  res.on("close", () => {
    const summary = {
      sessionId,
      elapsedMs: elapsed(),
      endCause,
      pollCount,
      totalChunksReceived,
      totalBytesReceived,
      totalBytesWritten,
      lastChunkSeqWritten,
      writableEnded: res.writableEnded,
      upstreamStatus,
      upstreamContentType,
    };
    tracer.emit("INFO", "request finished", summary);
    // Optional fire-and-forget recap → Node /api/csg/trace/recap so the
    // portal "CSG 追踪" tab can show CSG-side events alongside Node events
    // under the same traceId. Default OFF (requires CSG to also hold the
    // node's Proxy Key); enable with CSG_REPORT_TRACES=1 + CSG_PROXY_KEY=...
    if (REPORT_TRACES && REPORT_PROXY_KEY) {
      fetch(`${nodeBase}/api/csg/trace/recap`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${REPORT_PROXY_KEY}`,
        },
        body: JSON.stringify({ traceId, summary, csgEvents: tracer.events }),
      }).catch(() => {});
    }
  });

  // Stamp traceId into the main "request received" log so it shows up in
  // every CSG log line and can be cross-referenced with the node-side trace.
  // (We use plain log here; tracer.emit also includes traceId, but the
  // request-received line predates the per-request collector existing.)

  tracer.emit("INFO", "request received", {
    nodeHost,
    method,
    path: originalPath.split("?")[0],
    bodyBytes: body?.byteLength ?? 0,
    clientIp: req.socket?.remoteAddress ?? null,
    userAgent: req.headers["user-agent"] ?? null,
  });

  // ── Step 1: open session on the node ──────────────────────────────────────
  const startFetchAt = Date.now();
  let startResp: Response;
  try {
    startResp = await fetch(`${nodeBase}/csg${originalPath}`, {
      method: req.method ?? "POST",
      headers: forwardHeaders,
      // Buffer is a Uint8Array<ArrayBufferLike>; DOM BodyInit requires
      // ArrayBuffer (not ArrayBufferLike). Slice out the exact backing
      // region so fetch receives a proper ArrayBuffer.
      body: body
        ? (body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength) as ArrayBuffer)
        : undefined,
    });
  } catch (err) {
    log("ERROR", "csg start fetch failed", {
      nodeHost, elapsedMs: elapsed(),
      startFetchMs: Date.now() - startFetchAt,
      err: String(err),
    });
    endCause = "start_fetch_failed";
    sendError(res, 502, "node_unreachable", `Cannot reach node ${nodeHost}: ${String(err)}`);
    return;
  }

  // Non-2xx from /csg/<path> (e.g. 401 bad key, 429 cap) — relay directly
  // to the client before any poll loop starts.
  if (!startResp.ok) {
    const errBody = await startResp.text().catch(() => "");
    const ct = startResp.headers.get("content-type") ?? "application/json";
    log("WARN", "node refused session start", {
      nodeHost, status: startResp.status,
      startFetchMs: Date.now() - startFetchAt,
      bodySnippet: errBody.slice(0, 200),
    });
    endCause = `start_${startResp.status}`;
    res.writeHead(startResp.status, { ...CORS_HEADERS, "content-type": ct });
    res.end(errBody);
    return;
  }

  let startData: { sessionId: string; ttlMs: number };
  try {
    startData = (await startResp.json()) as typeof startData;
  } catch (err) {
    endCause = "invalid_start_response";
    sendError(res, 502, "invalid_start_response", `Node returned unparseable start response: ${String(err)}`);
    return;
  }
  sessionId = startData.sessionId;

  tracer.emit("INFO", "session opened", {
    nodeHost, sessionId,
    path: originalPath.split("?")[0],
    startFetchMs: Date.now() - startFetchAt,
    ttlMs: startData.ttlMs,
  });

  // ── Step 2: poll loop ──────────────────────────────────────────────────────
  let since = 0;
  let ack = 0;
  let headersWritten = false;
  // Set to true when the upstream response is text/event-stream (SSE). Used to
  // decide whether to inject `: keepalive` comment lines between polls so that
  // firewalls / proxies / NAT devices with a ~300 s idle-TCP timeout do not RST
  // the client connection during long silent thinking phases.
  let isSSE = false;

  while (!res.writableEnded && !cancelled) {
    pollCount++;
    const pollNum = pollCount;
    const pollStartedAt = Date.now();
    let pollResp: Response;
    try {
      pollResp = await fetch(`${nodeBase}/csg/pull/${sessionId}`, {
        method: "POST",
        headers: {
          authorization: authHeader,
          "content-type": "application/json",
          [TRACE_HEADER]: traceId,
        },
        body: JSON.stringify({ since, ack, maxWaitMs: MAX_WAIT_MS }),
      });
    } catch (err) {
      log("WARN", "poll fetch error", {
        sessionId, pollNum,
        elapsedMs: elapsed(),
        pollDurationMs: Date.now() - pollStartedAt,
        since, ack,
        err: String(err),
      });
      endCause = "poll_fetch_error";
      sendError(res, 502, "poll_fetch_error", String(err));
      cancelSession("poll_fetch_error");
      return;
    }

    if (pollResp.status === 404) {
      log("WARN", "session not found during poll", {
        sessionId, pollNum,
        elapsedMs: elapsed(),
        pollDurationMs: Date.now() - pollStartedAt,
        since, ack,
        totalBytesWritten,
      });
      endCause = "session_expired";
      sendError(res, 502, "session_expired", "Pull session expired on the node");
      sessionId = null;
      return;
    }

    if (!pollResp.ok) {
      log("WARN", "poll returned non-2xx", {
        sessionId, pollNum,
        status: pollResp.status,
        elapsedMs: elapsed(),
        pollDurationMs: Date.now() - pollStartedAt,
      });
      endCause = `poll_${pollResp.status}`;
      sendError(res, 502, "poll_error", `Node poll returned ${pollResp.status}`);
      cancelSession("poll_non_2xx");
      return;
    }

    let result: PollResult;
    try {
      result = (await pollResp.json()) as PollResult;
    } catch (err) {
      log("ERROR", "poll response parse failed", {
        sessionId, pollNum,
        elapsedMs: elapsed(),
        pollDurationMs: Date.now() - pollStartedAt,
        err: String(err),
      });
      endCause = "poll_parse_error";
      sendError(res, 502, "poll_parse_error", String(err));
      cancelSession("poll_parse_error");
      return;
    }

    const pollDurationMs = Date.now() - pollStartedAt;
    const chunkCount = result.chunks?.length ?? 0;
    const chunkBytes = (result.chunks ?? [])
      .reduce((sum, c) => sum + Math.floor((c.dataB64?.length ?? 0) * 3 / 4), 0);
    totalChunksReceived += chunkCount;
    totalBytesReceived += chunkBytes;

    // ── Write upstream headers on first response that carries `first` ──────
    if (!headersWritten && result.first) {
      const upstreamHeaders: Record<string, string> = {};
      for (const [k, v] of Object.entries(result.first.headers ?? {})) {
        const lower = k.toLowerCase();
        if (HOP_BY_HOP.has(lower)) continue;
        upstreamHeaders[k] = String(v);
      }
      upstreamStatus = result.first.status ?? 200;
      upstreamContentType = result.first.headers?.["content-type"] ?? null;
      isSSE = (upstreamContentType ?? "").includes("text/event-stream");
      try {
        res.writeHead(upstreamStatus, { ...CORS_HEADERS, ...upstreamHeaders });
      } catch (err) {
        log("WARN", "writeHead failed (client gone before headers)", {
          sessionId, pollNum, elapsedMs: elapsed(),
          err: String(err),
        });
        endCause = "write_head_failed";
        cancelSession("write_head_failed");
        return;
      }
      headersWritten = true;
      log("INFO", "upstream headers received", {
        sessionId, pollNum,
        elapsedMs: elapsed(),
        pollDurationMs,
        upstreamStatus,
        upstreamContentType,
        isSSE,
      });
    }

    // ── Write chunks in seq order ──────────────────────────────────────────
    const sortedChunks = (result.chunks ?? [])
      .filter((c) => c.seq >= since)
      .sort((a, b) => a.seq - b.seq);

    let maxSeq = -1;
    let bytesThisPoll = 0;
    for (const chunk of sortedChunks) {
      const buf = Buffer.from(chunk.dataB64, "base64");
      try {
        const ok = res.write(buf);
        // res.write returns false when internal buffer is full (backpressure).
        // We don't await drain — Node will keep buffering. But log it so we
        // can correlate slow clients with mid-stream stalls.
        if (!ok) {
          log("INFO", "client backpressure (write returned false)", {
            sessionId, pollNum, elapsedMs: elapsed(),
            seq: chunk.seq, totalBytesWritten,
          });
        }
      } catch (err) {
        log("WARN", "res.write threw (client gone mid-chunk)", {
          sessionId, pollNum,
          elapsedMs: elapsed(),
          seq: chunk.seq,
          chunkBytes: buf.byteLength,
          totalBytesWritten,
          msSinceLastWrite: Date.now() - lastWriteAt,
          err: String(err),
        });
        endCause = "write_threw";
        cancelSession("write_threw");
        return;
      }
      totalBytesWritten += buf.byteLength;
      bytesThisPoll += buf.byteLength;
      lastWriteAt = Date.now();
      lastChunkSeqWritten = chunk.seq;
      if (chunk.seq > maxSeq) maxSeq = chunk.seq;
    }

    if (maxSeq >= 0) {
      since = maxSeq + 1;
      ack = since;
    }

    log("INFO", "poll completed", {
      sessionId, pollNum,
      elapsedMs: elapsed(),
      pollDurationMs,
      since, ack,
      chunksThisPoll: chunkCount,
      bytesThisPoll,
      totalBytesWritten,
      done: result.done,
      hasError: Boolean(result.error),
    });

    // ── SSE keepalive ──────────────────────────────────────────────────────
    if (isSSE && headersWritten && sortedChunks.length === 0 && !result.done && !result.error) {
      try {
        res.write(": keepalive\n\n");
        lastWriteAt = Date.now();
      } catch (err) {
        log("WARN", "keepalive write threw (client gone)", {
          sessionId, pollNum, elapsedMs: elapsed(),
          err: String(err),
        });
        endCause = "keepalive_write_threw";
        cancelSession("keepalive_write_threw");
        return;
      }
    }

    // ── Handle upstream error (mid-stream fatal) ───────────────────────────
    if (result.error) {
      log("WARN", "upstream error mid-stream", {
        sessionId, pollNum,
        elapsedMs: elapsed(),
        phase: result.error.phase,
        message: result.error.message,
        totalBytesWritten,
        lastChunkSeqWritten,
      });
      endCause = `upstream_error_${result.error.phase ?? "unknown"}`;
      res.destroy();
      sessionId = null;
      return;
    }

    // ── Done ───────────────────────────────────────────────────────────────
    if (result.done) {
      log("INFO", "session done (upstream finished cleanly)", {
        sessionId, pollNum,
        elapsedMs: elapsed(),
        totalBytesWritten,
        lastChunkSeqWritten,
        upstreamStatus,
      });
      endCause = "upstream_done";
      sessionId = null;
      res.end();
      return;
    }
  }

  // Loop exited because res.writableEnded || cancelled. The req.on("close")
  // handler has already logged the cause; nothing more to do here.
}

// ─── Server ───────────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  handleRequest(req, res).catch((err) => {
    log("ERROR", "unhandled error in request handler", { err: String(err) });
    if (!res.headersSent) {
      try {
        res.writeHead(500, { ...CORS_HEADERS, "content-type": "application/json" });
        res.end(JSON.stringify({ error: { type: "internal_error", message: String(err) } }));
      } catch {
        res.destroy();
      }
    } else {
      res.destroy();
    }
  });
});

// The whole point of CSG is to hold the client connection open for the
// duration of a long AI stream (potentially 10+ minutes). Do not let Node's
// built-in keep-alive timers cut the connection.
server.keepAliveTimeout = 0;
server.requestTimeout = 0;
server.headersTimeout = 0;

server.listen(PORT, () => {
  log("INFO", "CSG listening", { port: PORT });
});

process.on("SIGTERM", () => {
  log("INFO", "SIGTERM received, shutting down");
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10_000);
});
process.on("SIGINT", () => {
  log("INFO", "SIGINT received, shutting down");
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10_000);
});
