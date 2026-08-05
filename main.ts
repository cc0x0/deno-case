// ============================================================================
// deno-showcase / main.ts
// 单文件 · 零第三方依赖 · 零构建
//
// 展示：Deno.serve / Deno KV (KV Watch 跨节点广播) / WebSocket / SSE / Web Crypto
//
// FILE-BEGIN
// ============================================================================

// -----------------------------------------------------------------------------
// 应用配置
// -----------------------------------------------------------------------------

const APP_NAME = "deno-showcase";
const BOOT_TIME = Date.now();
const INSTANCE_ID = crypto.randomUUID();

const MAX_HASH_BYTES = 16 * 1024;
const MAX_JSON_BYTES = 4 * 1024;
const MAX_WS_FRAME_BYTES = 2 * 1024;

const MAX_GUESTBOOK_NAME_LENGTH = 24;
const MAX_GUESTBOOK_TEXT_LENGTH = 200;
const MAX_CHAT_TEXT_LENGTH = 300;

const DEMO_TEXT =
  "你好！我是通过 Deno.serve 和 ReadableStream 实现的流式输出 ——" +
  "这正是聊天类应用中「打字机效果」的底层原理。" +
  "无需任何框架，Web 标准 API 直接在边缘运行。🦕⚡";

// -----------------------------------------------------------------------------
// 通用工具
// -----------------------------------------------------------------------------

class HttpError extends Error {
  status: number;
  headers?: HeadersInit;

  constructor(status: number, message: string, headers?: HeadersInit) {
    super(message);
    this.name = "HttpError";
    this.status = status;
    this.headers = headers;
  }
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function normalizeText(value: unknown, maxLength: number): string {
  return String(value ?? "")
    .replace(/\u0000/g, "")
    .trim()
    .slice(0, maxLength);
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function jsonResponse(
  data: unknown,
  init: ResponseInit = {},
): Response {
  const headers = new Headers(init.headers);

  headers.set("content-type", "application/json; charset=utf-8");
  headers.set("cache-control", "no-store");
  headers.set("x-content-type-options", "nosniff");

  return Response.json(data, {
    ...init,
    headers,
  });
}

function methodNotAllowed(methods: string[]): Response {
  return jsonResponse(
    {
      error: "Method Not Allowed",
      allowed: methods,
    },
    {
      status: 405,
      headers: {
        allow: methods.join(", "),
      },
    },
  );
}

function getClientAddress(req: Request): string {
  const forwarded = req.headers.get("x-forwarded-for");

  if (forwarded) {
    return forwarded.split(",")[0]?.trim() || "unknown";
  }

  return req.headers.get("x-real-ip") ??
    req.headers.get("cf-connecting-ip") ??
    "unknown";
}

async function readTextBody(
  req: Request,
  maxBytes: number,
): Promise<string> {
  const contentLength = Number(
    req.headers.get("content-length") ?? "0",
  );

  if (
    Number.isFinite(contentLength) &&
    contentLength > maxBytes
  ) {
    throw new HttpError(
      413,
      `请求体不能超过 ${maxBytes} 字节`,
    );
  }

  const body = await req.text();

  if (utf8Length(body) > maxBytes) {
    throw new HttpError(
      413,
      `请求体不能超过 ${maxBytes} 字节`,
    );
  }

  return body;
}

async function readJsonBody(
  req: Request,
  maxBytes: number,
): Promise<unknown> {
  const contentType = req.headers.get("content-type") ?? "";

  if (!contentType.toLowerCase().includes("application/json")) {
    throw new HttpError(
      415,
      "Content-Type 必须是 application/json",
    );
  }

  const raw = await readTextBody(req, maxBytes);

  if (!raw.trim()) {
    throw new HttpError(400, "请求体不能为空");
  }

  try {
    return JSON.parse(raw);
  } catch {
    throw new HttpError(400, "JSON 格式无效");
  }
}

// -----------------------------------------------------------------------------
// 单实例基础限流
// -----------------------------------------------------------------------------

type RateLimitRecord = {
  count: number;
  resetAt: number;
};

const rateLimits = new Map<string, RateLimitRecord>();

function checkRateLimit(
  req: Request,
  bucket: string,
  limit: number,
  windowMs: number,
): void {
  const now = Date.now();
  const key = `${bucket}:${getClientAddress(req)}`;

  let record = rateLimits.get(key);

  if (!record || record.resetAt <= now) {
    record = {
      count: 0,
      resetAt: now + windowMs,
    };
  }

  record.count += 1;
  rateLimits.set(key, record);

  if (record.count > limit) {
    const retryAfter = Math.max(
      1,
      Math.ceil((record.resetAt - now) / 1000),
    );

    throw new HttpError(
      429,
      `请求过于频繁，请在 ${retryAfter} 秒后重试`,
      {
        "retry-after": String(retryAfter),
      },
    );
  }

  if (rateLimits.size > 5_000) {
    for (const [entryKey, entry] of rateLimits) {
      if (entry.resetAt <= now) {
        rateLimits.delete(entryKey);
      }
    }
  }
}

// -----------------------------------------------------------------------------
// Deno KV 初始化与监听
// -----------------------------------------------------------------------------

let kv: Deno.Kv | null = null;
let kvApiAvailable = false;
let kvError: string | null = null;

async function initializeKv(): Promise<void> {
  kvApiAvailable = "openKv" in Deno && typeof Deno.openKv === "function";

  if (!kvApiAvailable) {
    kvError =
      "当前运行时未暴露 Deno.openKv。请确认数据库已连接到该 App，并在关联数据库后创建一次全新 Deployment。";

    console.error("[KV]", kvError);
    return;
  }

  try {
    kv = await Deno.openKv();
    kvError = null;
    console.log("[KV] 数据库初始化成功");

    setupKvWatch();
  } catch (error) {
    kv = null;
    kvError = getErrorMessage(error);
    console.error("[KV] 数据库初始化失败:", kvError);
  }
}

// -----------------------------------------------------------------------------
// Deno KV：访问计数
// -----------------------------------------------------------------------------

async function bumpVisits(): Promise<bigint | null> {
  if (!kv) return null;

  const result = await kv.atomic()
    .sum(["stats", "visits"], 1n)
    .commit();

  if (!result.ok) {
    throw new Error("KV 原子计数提交失败");
  }

  const current = await kv.get<Deno.KvU64>([
    "stats",
    "visits",
  ]);

  return current.value?.value ?? 0n;
}

// -----------------------------------------------------------------------------
// Deno KV：留言板
// -----------------------------------------------------------------------------

type GuestbookEntry = {
  id: string;
  name: string;
  text: string;
  ts: number;
};

async function addGuestbookEntry(
  name: string,
  text: string,
): Promise<GuestbookEntry | null> {
  if (!kv) return null;

  const id = crypto.randomUUID();
  const ts = Date.now();

  const entry: GuestbookEntry = {
    id,
    name,
    text,
    ts,
  };

  await kv.set(
    ["guestbook-v2", ts, id],
    entry,
  );

  return entry;
}

async function listGuestbook(
  limit = 20,
): Promise<GuestbookEntry[] | null> {
  if (!kv) return null;

  const safeLimit = Math.max(
    1,
    Math.min(Math.floor(limit), 50),
  );

  const entries: GuestbookEntry[] = [];

  for await (
    const item of kv.list<GuestbookEntry>(
      {
        prefix: ["guestbook-v2"],
      },
      {
        reverse: true,
        limit: safeLimit,
      },
    )
  ) {
    const value = item.value;

    if (
      value &&
      typeof value.id === "string" &&
      typeof value.name === "string" &&
      typeof value.text === "string" &&
      typeof value.ts === "number"
    ) {
      entries.push(value);
    }
  }

  return entries;
}

// -----------------------------------------------------------------------------
// SSE 流式响应
// -----------------------------------------------------------------------------

function createSseStream(
  text: string,
  signal: AbortSignal,
): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  const characters = Array.from(text);

  let index = 0;
  let stopped = false;

  return new ReadableStream<Uint8Array>({
    start(controller) {
      const handleAbort = () => {
        stopped = true;
        try {
          controller.close();
        } catch {
          // 流可能已经关闭
        }
      };

      if (signal.aborted) {
        handleAbort();
        return;
      }

      signal.addEventListener(
        "abort",
        handleAbort,
        { once: true },
      );
    },

    async pull(controller) {
      if (stopped || signal.aborted) {
        return;
      }

      if (index >= characters.length) {
        stopped = true;
        try {
          controller.enqueue(
            encoder.encode("event: done\ndata: {}\n\n"),
          );
          controller.close();
        } catch {
          // 忽略已关闭的情况
        }
        return;
      }

      const chunk = characters[index++];
      const payload = JSON.stringify({ chunk });

      try {
        controller.enqueue(
          encoder.encode(`data: ${payload}\n\n`),
        );
      } catch {
        stopped = true;
        return;
      }

      await new Promise((resolve) => {
        setTimeout(resolve, 28);
      });
    },

    cancel() {
      stopped = true;
    },
  });
}

// -----------------------------------------------------------------------------
// WebSocket & 跨节点实时同步
// -----------------------------------------------------------------------------

type WebSocketClient = {
  socket: WebSocket;
  name: string;
};

type ChatRealtimeMessage = {
  type: "chat";
  eventId: string;
  sourceInstanceId: string;
  sourceClientId: string;
  name: string;
  text: string;
  ts: number;
};

type SystemRealtimeMessage = {
  type: "system";
  eventId: string;
  sourceInstanceId: string;
  sourceClientId?: string;
  text: string;
  ts: number;
};

type RealtimeMessage =
  | ChatRealtimeMessage
  | SystemRealtimeMessage;

const clients = new Map<string, WebSocketClient>();

function randomName(): string {
  const adjectives = [
    "敏捷的",
    "神秘的",
    "闪电",
    "深海",
    "静谧的",
    "赛博",
    "量子",
  ];

  const nouns = [
    "恐龙",
    "旅人",
    "信使",
    "浣熊",
    "游侠",
    "观察者",
    "锦鲤",
  ];

  const adjective =
    adjectives[Math.floor(Math.random() * adjectives.length)];

  const noun =
    nouns[Math.floor(Math.random() * nouns.length)];

  const suffix = Math.floor(Math.random() * 100);

  return `${adjective}${noun}${suffix}`;
}

function sendSocket(
  socket: WebSocket,
  payload: unknown,
): void {
  if (socket.readyState !== WebSocket.OPEN) {
    return;
  }

  try {
    socket.send(JSON.stringify(payload));
  } catch {
    // 连接可能刚好关闭
  }
}

function sendLocalPresence(): void {
  const message = {
    type: "presence",
    online: clients.size,
    scope: "instance",
  };

  for (const client of clients.values()) {
    sendSocket(client.socket, message);
  }
}

function broadcastToLocalClients(
  message: RealtimeMessage,
): void {
  for (const [clientId, client] of clients) {
    if (message.type === "chat") {
      sendSocket(client.socket, {
        type: "chat",
        name: message.name,
        text: message.text,
        ts: message.ts,
        self: message.sourceClientId === clientId,
      });

      continue;
    }

    sendSocket(client.socket, {
      type: "system",
      text: message.text,
      ts: message.ts,
    });
  }
}

function publishRealtime(
  message: RealtimeMessage,
): void {
  broadcastToLocalClients(message);

  if (kv) {
    kv.set(["realtime", "latest_msg"], message).catch((err) => {
      console.error("[KV Realtime] 广播消息写入失败:", getErrorMessage(err));
    });
  }
}

async function setupKvWatch(): Promise<void> {
  if (!kv) return;

  try {
    const watcher = kv.watch<[RealtimeMessage]>([["realtime", "latest_msg"]]);
    for await (const [entry] of watcher) {
      if (entry.value) {
        const message = entry.value;
        if (message.sourceInstanceId === INSTANCE_ID) {
          continue;
        }
        broadcastToLocalClients(message);
      }
    }
  } catch (error) {
    console.error("[KV Watch] 监听异常:", getErrorMessage(error));
  }
}

await initializeKv();

// -----------------------------------------------------------------------------
// WebSocket Upgrade
// -----------------------------------------------------------------------------

function handleWebSocket(req: Request): Response {
  const upgrade = req.headers.get("upgrade");

  if (
    !upgrade ||
    upgrade.toLowerCase() !== "websocket"
  ) {
    return new Response(
      "Expected WebSocket upgrade",
      {
        status: 426,
        headers: {
          upgrade: "websocket",
        },
      },
    );
  }

  const { socket, response } =
    Deno.upgradeWebSocket(req);

  const clientId = crypto.randomUUID();
  const name = randomName();

  let messageWindowStartedAt = Date.now();
  let messageCount = 0;

  socket.onopen = () => {
    clients.set(clientId, {
      socket,
      name,
    });

    sendSocket(socket, {
      type: "identity",
      name,
      clientId,
      instanceId: INSTANCE_ID,
    });

    publishRealtime({
      type: "system",
      eventId: crypto.randomUUID(),
      sourceInstanceId: INSTANCE_ID,
      sourceClientId: clientId,
      text: `${name} 加入了聊天`,
      ts: Date.now(),
    });

    sendLocalPresence();
  };

  socket.onmessage = (
    event: MessageEvent,
  ) => {
    try {
      if (typeof event.data !== "string") {
        sendSocket(socket, {
          type: "system",
          text: "只接受文本格式的 WebSocket 消息",
        });

        return;
      }

      if (
        utf8Length(event.data) >
          MAX_WS_FRAME_BYTES
      ) {
        sendSocket(socket, {
          type: "system",
          text: "消息数据过大",
        });

        return;
      }

      const now = Date.now();

      if (
        now - messageWindowStartedAt >=
          10_000
      ) {
        messageWindowStartedAt = now;
        messageCount = 0;
      }

      messageCount += 1;

      if (messageCount > 15) {
        sendSocket(socket, {
          type: "system",
          text: "发送过于频繁，请稍后再试",
        });

        return;
      }

      const parsed = JSON.parse(
        event.data,
      ) as {
        text?: unknown;
      };

      const text = normalizeText(
        parsed.text,
        MAX_CHAT_TEXT_LENGTH,
      );

      if (!text) return;

      publishRealtime({
        type: "chat",
        eventId: crypto.randomUUID(),
        sourceInstanceId: INSTANCE_ID,
        sourceClientId: clientId,
        name,
        text,
        ts: now,
      });
    } catch {
      sendSocket(socket, {
        type: "system",
        text: "无法解析消息",
      });
    }
  };

  socket.onerror = (event) => {
    console.error(
      "[WebSocket] 连接异常:",
      clientId,
      event,
    );
  };

  socket.onclose = () => {
    const existed = clients.delete(clientId);

    if (!existed) return;

    publishRealtime({
      type: "system",
      eventId: crypto.randomUUID(),
      sourceInstanceId: INSTANCE_ID,
      sourceClientId: clientId,
      text: `${name} 离开了聊天`,
      ts: Date.now(),
    });

    sendLocalPresence();
  };

  return response;
}

// -----------------------------------------------------------------------------
// 单文件页面 HTML & 现代 Chat UI
// -----------------------------------------------------------------------------

const PAGE_HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<meta name="color-scheme" content="dark" />
<title>🦕 Deno Deploy 能力全景秀</title>

<style>
  :root {
    --bg: #0a0e14;
    --panel: rgba(255, 255, 255, 0.05);
    --border: rgba(255, 255, 255, 0.1);
    --accent: #00d4aa;
    --accent2: #7c5cff;
    --text: #e8ecf1;
    --muted: #8b95a5;
    --warning: #ffbd5a;
    --error: #ff6b7a;
  }

  * {
    box-sizing: border-box;
  }

  html,
  body {
    margin: 0;
    padding: 0;
    overflow-x: hidden;
    background: var(--bg);
    color: var(--text);
    font-family:
      "SF Pro Display",
      -apple-system,
      BlinkMacSystemFont,
      "Segoe UI",
      Roboto,
      "PingFang SC",
      "Microsoft YaHei",
      sans-serif;
  }

  button,
  input,
  textarea {
    font: inherit;
  }

  canvas#bg {
    position: fixed;
    inset: 0;
    z-index: 0;
    pointer-events: none;
  }

  .wrap {
    position: relative;
    z-index: 1;
    width: 100%;
    max-width: 960px;
    margin: 0 auto;
    padding: 48px 20px 100px;
  }

  header {
    margin-bottom: 56px;
    text-align: center;
  }

  header h1 {
    margin: 0 0 12px;
    background:
      linear-gradient(
        120deg,
        var(--accent),
        var(--accent2)
      );
    background-clip: text;
    -webkit-background-clip: text;
    color: transparent;
    font-size: clamp(28px, 5vw, 44px);
    font-weight: 800;
    letter-spacing: -0.02em;
  }

  header p {
    margin: 0;
    color: var(--muted);
    font-size: 16px;
    line-height: 1.7;
  }

  .badges {
    display: flex;
    flex-wrap: wrap;
    justify-content: center;
    gap: 8px;
    margin-top: 20px;
  }

  .badge {
    padding: 5px 12px;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--panel);
    color: var(--muted);
    font-family:
      "SF Mono",
      Menlo,
      Consolas,
      monospace;
    font-size: 12px;
  }

  .grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
  }

  .card {
    position: relative;
    overflow: hidden;
    padding: 24px;
    border: 1px solid var(--border);
    border-radius: 16px;
    background: var(--panel);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    transition:
      transform 0.25s ease,
      border-color 0.25s ease;
  }

  .card:hover {
    transform: translateY(-3px);
    border-color: rgba(0, 212, 170, 0.4);
  }

  .card h2 {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 8px;
    margin: 0 0 6px;
    font-size: 17px;
  }

  .card .desc {
    margin-bottom: 16px;
    color: var(--muted);
    font-size: 13px;
    line-height: 1.65;
  }

  .full {
    grid-column: 1 / -1;
  }

  .stats-row {
    display: flex;
    flex-wrap: wrap;
    gap: 28px;
  }

  .stat-block {
    min-width: 100px;
  }

  .stat {
    color: var(--accent);
    font-family:
      "SF Mono",
      Menlo,
      Consolas,
      monospace;
    font-size: 34px;
    font-weight: 800;
    overflow-wrap: anywhere;
  }

  .stat.small {
    font-size: 16px;
  }

  .stat-label {
    margin-top: 4px;
    color: var(--muted);
    font-size: 12px;
  }

  .row {
    display: flex;
    gap: 8px;
  }

  input,
  textarea,
  button {
    padding: 9px 12px;
    border: 1px solid var(--border);
    border-radius: 8px;
    outline: none;
  }

  input,
  textarea {
    min-width: 0;
    flex: 1;
    background: rgba(255, 255, 255, 0.06);
    color: var(--text);
  }

  input:focus,
  textarea:focus {
    border-color: var(--accent);
  }

  button {
    border: none;
    cursor: pointer;
    background:
      linear-gradient(
        120deg,
        var(--accent),
        var(--accent2)
      );
    color: #04140f;
    font-weight: 700;
    transition:
      opacity 0.2s ease,
      transform 0.2s ease;
  }

  button:hover:not(:disabled) {
    opacity: 0.86;
  }

  button:active:not(:disabled) {
    transform: scale(0.98);
  }

  button:disabled {
    cursor: not-allowed;
    opacity: 0.4;
  }

  .notice {
    padding: 10px 12px;
    border: 1px solid rgba(255, 189, 90, 0.25);
    border-radius: 8px;
    background: rgba(255, 189, 90, 0.07);
    color: var(--warning);
    font-size: 12px;
    line-height: 1.65;
    overflow-wrap: anywhere;
  }

  .hidden {
    display: none !important;
  }

  .status {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    color: var(--muted);
    font-size: 12px;
    font-weight: 400;
  }

  .status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--warning);
  }

  .status.connected .status-dot {
    background: var(--accent);
    box-shadow: 0 0 8px rgba(0, 212, 170, 0.8);
  }

  .status.disconnected .status-dot {
    background: var(--error);
  }

  /* -------------------------------------------------------------------------
   * 现代 Chat UI 样式 (Bubble & Flex 布局)
   * ------------------------------------------------------------------------- */
  #chatlog {
    display: flex;
    flex-direction: column;
    gap: 12px;
    height: 280px;
    overflow-y: auto;
    margin-bottom: 12px;
    padding: 16px;
    border-radius: 12px;
    background: rgba(0, 0, 0, 0.35);
    border: 1px solid rgba(255, 255, 255, 0.05);
  }

  .msg-row {
    display: flex;
    flex-direction: column;
    max-width: 80%;
  }

  /* 1. 系统消息：居中展示 */
  .msg-row.sys-row {
    align-self: center;
    max-width: 90%;
    margin: 4px 0;
  }

  .sys-badge {
    padding: 4px 12px;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.06);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: var(--muted);
    font-size: 11px;
    text-align: center;
  }

  /* 2. 别人的消息：左对齐 */
  .msg-row.other-row {
    align-self: flex-start;
  }

  .msg-author {
    font-size: 11px;
    color: var(--muted);
    margin-bottom: 4px;
    padding-left: 4px;
  }

  .other-row .msg-bubble {
    background: rgba(255, 255, 255, 0.08);
    border: 1px solid rgba(255, 255, 255, 0.1);
    color: var(--text);
    border-top-left-radius: 4px;
  }

  /* 3. 自己的消息：右对齐 */
  .msg-row.me-row {
    align-self: flex-end;
  }

  .me-row .msg-bubble {
    background: linear-gradient(135deg, var(--accent), #00a887);
    color: #04140f;
    font-weight: 500;
    border-top-right-radius: 4px;
    box-shadow: 0 2px 10px rgba(0, 212, 170, 0.2);
  }

  /* 消息气泡通用结构 */
  .msg-bubble {
    position: relative;
    padding: 10px 14px;
    border-radius: 14px;
    font-size: 13.5px;
    line-height: 1.5;
    word-break: break-word;
  }

  .msg-time {
    display: inline-block;
    font-size: 10px;
    margin-left: 8px;
    opacity: 0.65;
    float: right;
    margin-top: 4px;
  }

  #stream-out {
    min-height: 60px;
    margin-top: 10px;
    padding: 14px;
    border-radius: 10px;
    background: rgba(0, 0, 0, 0.3);
    font-size: 14px;
    line-height: 1.7;
    overflow-wrap: anywhere;
  }

  #stream-out .cursor {
    display: inline-block;
    width: 8px;
    height: 16px;
    margin-left: 2px;
    vertical-align: middle;
    background: var(--accent);
    animation: blink 1s step-end infinite;
  }

  @keyframes blink {
    50% {
      opacity: 0;
    }
  }

  #guestbook-list {
    max-height: 260px;
    overflow-y: auto;
    margin-top: 14px;
  }

  .gb-entry {
    padding: 10px 0;
    border-bottom: 1px solid var(--border);
    font-size: 13px;
    line-height: 1.55;
    overflow-wrap: anywhere;
  }

  .gb-entry b {
    color: var(--accent);
  }

  .gb-time {
    margin-top: 3px;
    color: var(--muted);
    font-size: 11px;
  }

  #hash-out {
    min-height: 36px;
    margin-top: 10px;
    padding: 10px;
    border-radius: 8px;
    background: rgba(0, 0, 0, 0.3);
    color: var(--accent);
    font-family:
      "SF Mono",
      Menlo,
      Consolas,
      monospace;
    font-size: 12px;
    line-height: 1.5;
    overflow-wrap: anywhere;
  }

  footer {
    margin-top: 60px;
    color: var(--muted);
    text-align: center;
    font-size: 12px;
    line-height: 1.8;
  }

  footer a {
    color: var(--accent);
    text-decoration: none;
  }

  footer a:hover {
    text-decoration: underline;
  }

  @media (max-width: 760px) {
    .grid {
      grid-template-columns: 1fr;
    }

    .row.mobile-stack {
      flex-direction: column;
    }

    #gb-name {
      max-width: none !important;
    }

    .msg-row {
      max-width: 90%;
    }
  }

  @media (prefers-reduced-motion: reduce) {
    canvas#bg {
      display: none;
    }

    .card {
      transition: none;
    }

    #stream-out .cursor {
      animation: none;
    }
  }
</style>
</head>

<body>
<canvas id="bg"></canvas>

<div class="wrap">
  <header>
    <h1>🦕 Deno Deploy 能力全景秀</h1>
    <p>
      一个 main.ts，零构建，全部运行在边缘——这个页面本身就是证据
    </p>
    <div class="badges" id="badges"></div>
  </header>

  <main class="grid">
    <section class="card full">
      <h2>⚡ 实时运行时信息</h2>

      <div class="desc">
        直接从 Deno.version 和 Deno.env 读取，服务端每次请求实时计算
      </div>

      <div
        class="stats-row"
        id="runtime-stats"
      ></div>

      <div
        id="kv-warning"
        class="notice hidden"
        style="margin-top:16px"
      ></div>
    </section>

    <section class="card">
      <h2>📈 Deno KV 原子计数器</h2>

      <div class="desc">
        每次打开页面调用 kv.atomic().sum() 持久化 +1
      </div>

      <div
        class="stat"
        id="visit-count"
      >–</div>

      <div
        class="stat-label"
        id="visit-label"
      >
        累计访问次数（跨实例持久化）
      </div>
    </section>

    <section class="card">
      <h2>🔐 Web Crypto API</h2>

      <div class="desc">
        服务端调用 crypto.subtle.digest 计算 SHA-256
      </div>

      <div class="row">
        <input
          id="hash-input"
          maxlength="4096"
          placeholder="输入任意文本..."
          value="Hello Deno"
        />

        <button
          id="hash-btn"
          type="button"
        >
          SHA-256
        </button>
      </div>

      <div id="hash-out">等待输入…</div>
    </section>

    <section class="card full">
      <h2>
        💬 WebSocket 实时中转

        <span
          id="ws-status"
          class="status"
        >
          <span class="status-dot"></span>
          <span id="ws-status-text">连接中</span>
        </span>
      </h2>

      <div class="desc">
        浏览器 ⇄ Deno.serve WebSocket ⇄ Deno KV Watch 跨边缘中转。
        打开两个网页即可实时通信。当前边缘节点连接：
        <b id="online-count">0</b>
      </div>

      <div
        id="chatlog"
        aria-live="polite"
      ></div>

      <div class="row">
        <input
          id="chat-input"
          maxlength="300"
          placeholder="输入消息，回车发送..."
          autocomplete="off"
        />

        <button
          id="chat-send"
          type="button"
          disabled
        >
          发送
        </button>
      </div>
    </section>

    <section class="card full">
      <h2>🌊 SSE 流式响应</h2>

      <div class="desc">
        用 ReadableStream 逐字符 enqueue，模拟大模型 token-by-token 输出
      </div>

      <button
        id="stream-btn"
        type="button"
      >
        ▶ 开始流式输出
      </button>

      <div id="stream-out">
        点击按钮查看效果...
      </div>
    </section>

    <section class="card full">
      <h2>📝 Deno KV 留言板</h2>

      <div class="desc">
        使用 kv.set() 写入、kv.list() 按时间倒序读取
      </div>

      <div class="row mobile-stack">
        <input
          id="gb-name"
          maxlength="24"
          placeholder="你的名字"
          style="max-width:140px"
        />

        <input
          id="gb-text"
          maxlength="200"
          placeholder="留下点什么..."
        />

        <button
          id="gb-send"
          type="button"
        >
          提交
        </button>
      </div>

      <div id="guestbook-list"></div>
    </section>
  </main>

  <footer>
    由 <b>Deno.serve</b> 直接驱动，无框架、无构建步骤、无 node_modules
    <br />

    <a
      href="/api/info"
      target="_blank"
      rel="noopener"
    >
      查看运行时 JSON
    </a>

    ·

    <a
      href="/api/diagnostics"
      target="_blank"
      rel="noopener"
    >
      查看部署诊断
    </a>

    ·

    <a
      href="/health"
      target="_blank"
      rel="noopener"
    >
      健康检查
    </a>
  </footer>
</div>

<script>
(function () {
  "use strict";

  function byId(id) {
    return document.getElementById(id);
  }

  function setText(id, value) {
    var element = byId(id);

    if (element) {
      element.textContent = String(value);
    }
  }

  function requestJson(url, options) {
    return fetch(url, options).then(function (response) {
      return response.json()
        .catch(function () {
          return {
            error: "服务器返回了无效 JSON"
          };
        })
        .then(function (body) {
          if (!response.ok) {
            var message =
              body && body.error
                ? body.error
                : "请求失败：" + response.status;

            var error = new Error(message);
            error.status = response.status;
            error.body = body;

            throw error;
          }

          return body;
        });
    });
  }

  // ---------------------------------------------------------------------------
  // Canvas 背景
  // ---------------------------------------------------------------------------

  (function setupBackground() {
    if (
      window.matchMedia &&
      window.matchMedia(
        "(prefers-reduced-motion: reduce)"
      ).matches
    ) {
      return;
    }

    var canvas = byId("bg");
    var context = canvas.getContext("2d");

    if (!context) return;

    var width = 0;
    var height = 0;
    var particles = [];

    function createParticles() {
      var count = Math.min(
        70,
        Math.floor(
          (width * height) / 18000
        )
      );

      particles = [];

      for (var index = 0; index < count; index++) {
        particles.push({
          x: Math.random() * width,
          y: Math.random() * height,
          vx: (Math.random() - 0.5) * 0.35,
          vy: (Math.random() - 0.5) * 0.35
        });
      }
    }

    function resize() {
      var ratio = Math.min(
        window.devicePixelRatio || 1,
        2
      );

      width = window.innerWidth;
      height = window.innerHeight;

      canvas.width = Math.floor(width * ratio);
      canvas.height = Math.floor(height * ratio);
      canvas.style.width = width + "px";
      canvas.style.height = height + "px";

      context.setTransform(
        ratio,
        0,
        0,
        ratio,
        0,
        0
      );

      createParticles();
    }

    function tick() {
      context.clearRect(
        0,
        0,
        width,
        height
      );

      context.fillStyle =
        "rgba(0,212,170,0.55)";

      for (
        var index = 0;
        index < particles.length;
        index++
      ) {
        var particle = particles[index];

        particle.x += particle.vx;
        particle.y += particle.vy;

        if (
          particle.x < 0 ||
          particle.x > width
        ) {
          particle.vx *= -1;
        }

        if (
          particle.y < 0 ||
          particle.y > height
        ) {
          particle.vy *= -1;
        }

        context.beginPath();
        context.arc(
          particle.x,
          particle.y,
          1.6,
          0,
          Math.PI * 2
        );
        context.fill();
      }

      for (
        var first = 0;
        first < particles.length;
        first++
      ) {
        for (
          var second = first + 1;
          second < particles.length;
          second++
        ) {
          var deltaX =
            particles[first].x -
            particles[second].x;

          var deltaY =
            particles[first].y -
            particles[second].y;

          var distance = Math.sqrt(
            deltaX * deltaX +
            deltaY * deltaY
          );

          if (distance < 130) {
            context.strokeStyle =
              "rgba(124,92,255," +
              (
                0.18 *
                (1 - distance / 130)
              ) +
              ")";

            context.lineWidth = 1;
            context.beginPath();

            context.moveTo(
              particles[first].x,
              particles[first].y
            );

            context.lineTo(
              particles[second].x,
              particles[second].y
            );

            context.stroke();
          }
        }
      }

      requestAnimationFrame(tick);
    }

    resize();

    window.addEventListener(
      "resize",
      resize
    );

    requestAnimationFrame(tick);
  })();

  // ---------------------------------------------------------------------------
  // 技术徽章
  // ---------------------------------------------------------------------------

  var badgeNames = [
    "Deno.serve",
    "WebSocket",
    "BroadcastChannel",
    "Deno KV",
    "Server-Sent Events",
    "Web Crypto",
    "V8 Isolate",
    "零构建"
  ];

  badgeNames.forEach(function (name) {
    var badge = document.createElement("span");

    badge.className = "badge";
    badge.textContent = name;

    byId("badges").appendChild(badge);
  });

  // ---------------------------------------------------------------------------
  // 运行时信息
  // ---------------------------------------------------------------------------

  requestJson("/api/info")
    .then(function (info) {
      var items = [
        ["Deno 版本", info.denoVersion],
        ["V8 引擎", info.v8Version],
        ["TypeScript", info.tsVersion],
        ["运行区域", info.region],
        ["实例存活", info.uptime + "s"],
        [
          "KV 状态",
          info.kvAvailable
            ? "✅ 已连接"
            : "⚠️ 不可用"
        ]
      ];

      var container =
        byId("runtime-stats");

      items.forEach(function (item) {
        var block =
          document.createElement("div");

        var value =
          document.createElement("div");

        var label =
          document.createElement("div");

        block.className = "stat-block";
        value.className = "stat small";
        label.className = "stat-label";

        value.textContent = String(item[1]);
        label.textContent = item[0];

        block.appendChild(value);
        block.appendChild(label);
        container.appendChild(block);
      });

      if (!info.kvAvailable) {
        var warning = byId("kv-warning");

        warning.classList.remove("hidden");

        warning.textContent =
          "KV 不可用：" +
          (
            info.kvError ||
            "未获得具体错误信息"
          );
      }
    })
    .catch(function (error) {
      var warning = byId("kv-warning");

      warning.classList.remove("hidden");
      warning.textContent =
        "运行时信息加载失败：" +
        error.message;
    });

  // ---------------------------------------------------------------------------
  // 访问计数
  // ---------------------------------------------------------------------------

  requestJson("/api/visit", {
    method: "POST"
  })
    .then(function (data) {
      setText(
        "visit-count",
        data.count
      );
    })
    .catch(function (error) {
      var count = byId("visit-count");

      count.textContent = "—";
      count.style.color = "#8b95a5";
      count.style.fontSize = "16px";

      setText(
        "visit-label",
        error.status === 503
          ? "KV 未挂载或 KV API 未启用"
          : "访问计数加载失败：" +
            error.message
      );
    });

  // ---------------------------------------------------------------------------
  // SHA-256
  // ---------------------------------------------------------------------------

  function calculateHash() {
    var button = byId("hash-btn");
    var output = byId("hash-out");
    var input = byId("hash-input");
    var text = input.value;

    button.disabled = true;
    output.textContent = "计算中…";

    requestJson("/api/hash", {
      method: "POST",
      headers: {
        "content-type":
          "text/plain; charset=utf-8"
      },
      body: text
    })
      .then(function (data) {
        output.textContent = data.sha256;
      })
      .catch(function (error) {
        output.textContent =
          "错误：" + error.message;
      })
      .finally(function () {
        button.disabled = false;
      });
  }

  byId("hash-btn").addEventListener(
    "click",
    calculateHash
  );

  byId("hash-input").addEventListener(
    "keydown",
    function (event) {
      if (
        event.key === "Enter" &&
        !event.isComposing
      ) {
        calculateHash();
      }
    }
  );

  // ---------------------------------------------------------------------------
  // SSE
  // ---------------------------------------------------------------------------

  var activeEventSource = null;

  byId("stream-btn").addEventListener(
    "click",
    function () {
      var output = byId("stream-out");
      var button = byId("stream-btn");

      if (activeEventSource) {
        activeEventSource.close();
        activeEventSource = null;
      }

      output.textContent = "";

      var initialCursor =
        document.createElement("span");

      initialCursor.className = "cursor";
      output.appendChild(initialCursor);

      button.disabled = true;

      var eventSource =
        new EventSource("/api/stream");

      activeEventSource = eventSource;

      var buffer = "";
      var completed = false;

      function render(showCursor) {
        output.textContent = buffer;

        if (showCursor) {
          var cursor =
            document.createElement("span");

          cursor.className = "cursor";
          output.appendChild(cursor);
        }
      }

      eventSource.onmessage = function (event) {
        try {
          var data = JSON.parse(event.data);

          if (
            typeof data.chunk === "string"
          ) {
            buffer += data.chunk;
            render(true);
          }
        } catch (_) {
          // 忽略格式错误项
        }
      };

      eventSource.addEventListener(
        "done",
        function () {
          completed = true;
          render(false);
          eventSource.close();

          if (
            activeEventSource === eventSource
          ) {
            activeEventSource = null;
          }

          button.disabled = false;
        }
      );

      eventSource.onerror = function () {
        eventSource.close();

        if (
          activeEventSource === eventSource
        ) {
          activeEventSource = null;
        }

        if (!completed) {
          if (!buffer) {
            output.textContent =
              "流式连接失败，请稍后重试。";
          } else {
            render(false);
          }
        }

        button.disabled = false;
      };
    }
  );

  // ---------------------------------------------------------------------------
  // WebSocket (现代聊天气泡与系统提示渲染)
  // ---------------------------------------------------------------------------

  (function setupWebSocket() {
    var socket = null;
    var reconnectTimer = null;
    var reconnectAttempt = 0;
    var manuallyClosed = false;

    var log = byId("chatlog");
    var input = byId("chat-input");
    var sendButton = byId("chat-send");
    var status = byId("ws-status");
    var statusText = byId(
      "ws-status-text"
    );

    function formatTime(ts) {
      var date = ts ? new Date(ts) : new Date();
      var hours = String(date.getHours()).padStart(2, '0');
      var mins = String(date.getMinutes()).padStart(2, '0');
      return hours + ":" + mins;
    }

    // 格式化渲染：系统居中提示、自己右对齐气泡、别人左对齐气泡
    function addChatMessage(msg) {
      var row = document.createElement("div");

      if (msg.type === "system") {
        row.className = "msg-row sys-row";

        var badge = document.createElement("div");
        badge.className = "sys-badge";
        badge.textContent = msg.text;

        row.appendChild(badge);
      } else if (msg.type === "chat") {
        row.className = "msg-row " + (msg.self ? "me-row" : "other-row");

        if (!msg.self && msg.name) {
          var author = document.createElement("div");
          author.className = "msg-author";
          author.textContent = msg.name;
          row.appendChild(author);
        }

        var bubble = document.createElement("div");
        bubble.className = "msg-bubble";

        var textNode = document.createTextNode(msg.text);
        var timeSpan = document.createElement("span");
        timeSpan.className = "msg-time";
        timeSpan.textContent = formatTime(msg.ts);

        bubble.appendChild(textNode);
        bubble.appendChild(timeSpan);
        row.appendChild(bubble);
      }

      log.appendChild(row);
      log.scrollTop = log.scrollHeight;

      while (log.children.length > 200) {
        log.removeChild(log.firstChild);
      }
    }

    function setStatus(
      state,
      text
    ) {
      status.className =
        "status" +
        (state ? " " + state : "");

      statusText.textContent = text;
    }

    function scheduleReconnect() {
      if (
        manuallyClosed ||
        reconnectTimer
      ) {
        return;
      }

      reconnectAttempt += 1;

      var delay = Math.min(
        1000 * Math.pow(
          2,
          reconnectAttempt - 1
        ),
        15000
      );

      setStatus(
        "disconnected",
        "将在 " +
          Math.ceil(delay / 1000) +
          "s 后重连"
      );

      reconnectTimer =
        window.setTimeout(
          function () {
            reconnectTimer = null;
            connect();
          },
          delay
        );
    }

    function connect() {
      if (
        socket &&
        (
          socket.readyState ===
            WebSocket.OPEN ||
          socket.readyState ===
            WebSocket.CONNECTING
        )
      ) {
        return;
      }

      setStatus("", "连接中");
      sendButton.disabled = true;

      var protocol =
        location.protocol === "https:"
          ? "wss:"
          : "ws:";

      socket = new WebSocket(
        protocol +
          "//" +
          location.host +
          "/ws"
      );

      socket.onopen = function () {
        reconnectAttempt = 0;

        setStatus(
          "connected",
          "已连接"
        );

        sendButton.disabled = false;
      };

      socket.onmessage = function (event) {
        var message;

        try {
          message = JSON.parse(event.data);
        } catch (_) {
          return;
        }

        if (message.type === "identity") {
          addChatMessage({
            type: "system",
            text: "你的随机代号是: " + message.name
          });
          return;
        }

        if (message.type === "presence") {
          if (
            typeof message.online ===
              "number"
          ) {
            setText(
              "online-count",
              message.online
            );
          }

          return;
        }

        if (message.type === "system" || message.type === "chat") {
          addChatMessage(message);
        }
      };

      socket.onerror = function () {
        setStatus(
          "disconnected",
          "连接异常"
        );
      };

      socket.onclose = function () {
        sendButton.disabled = true;

        setText(
          "online-count",
          "0"
        );

        scheduleReconnect();
      };
    }

    function send() {
      var value = input.value.trim();

      if (!value) return;

      if (
        !socket ||
        socket.readyState !==
          WebSocket.OPEN
      ) {
        addChatMessage({
          type: "system",
          text: "WebSocket 尚未连接"
        });

        return;
      }

      socket.send(
        JSON.stringify({
          text: value
        })
      );

      input.value = "";
      input.focus();
    }

    sendButton.addEventListener(
      "click",
      send
    );

    input.addEventListener(
      "keydown",
      function (event) {
        if (
          event.key === "Enter" &&
          !event.isComposing
        ) {
          event.preventDefault();
          send();
        }
      }
    );

    window.addEventListener(
      "beforeunload",
      function () {
        manuallyClosed = true;

        if (reconnectTimer) {
          clearTimeout(reconnectTimer);
        }

        if (socket) {
          socket.close(
            1000,
            "page unload"
          );
        }
      }
    );

    connect();
  })();

  // ---------------------------------------------------------------------------
  // 留言板
  // ---------------------------------------------------------------------------

  function renderGuestbook(entries) {
    var container =
      byId("guestbook-list");

    container.textContent = "";

    if (!entries.length) {
      var empty =
        document.createElement("div");

      empty.className = "gb-entry";
      empty.style.color = "#8b95a5";
      empty.textContent =
        "还没有留言，来第一个吧～";

      container.appendChild(empty);
      return;
    }

    entries.forEach(function (entry) {
      var wrapper =
        document.createElement("div");

      var name =
        document.createElement("b");

      var separator =
        document.createTextNode(": ");

      var text =
        document.createTextNode(
          String(entry.text)
        );

      var time =
        document.createElement("div");

      wrapper.className = "gb-entry";
      name.textContent = String(entry.name);
      time.className = "gb-time";

      var timestamp = Number(entry.ts);

      if (Number.isFinite(timestamp)) {
        time.textContent =
          new Date(timestamp)
            .toLocaleString("zh-CN");
      } else {
        time.textContent = "未知时间";
      }

      wrapper.appendChild(name);
      wrapper.appendChild(separator);
      wrapper.appendChild(text);
      wrapper.appendChild(time);
      container.appendChild(wrapper);
    });
  }

  function loadGuestbook() {
    var container =
      byId("guestbook-list");

    container.textContent = "正在加载…";

    return requestJson("/api/guestbook")
      .then(function (entries) {
        if (!Array.isArray(entries)) {
          throw new Error(
            "留言板返回格式无效"
          );
        }

        renderGuestbook(entries);
      })
      .catch(function (error) {
        container.textContent = "";

        var warning =
          document.createElement("div");

        warning.className = "gb-entry";
        warning.style.color = "#8b95a5";
        warning.textContent =
          "⚠️ " + error.message;

        container.appendChild(warning);

        if (error.status === 503) {
          byId("gb-send").disabled = true;
        }

        throw error;
      });
  }

  function submitGuestbook() {
    var button = byId("gb-send");
    var nameInput = byId("gb-name");
    var textInput = byId("gb-text");

    var name =
      nameInput.value.trim() ||
      "匿名";

    var text =
      textInput.value.trim();

    if (!text) {
      textInput.focus();
      return;
    }

    button.disabled = true;
    button.textContent = "提交中…";

    requestJson(
      "/api/guestbook",
      {
        method: "POST",
        headers: {
          "content-type":
            "application/json"
        },
        body: JSON.stringify({
          name: name,
          text: text
        })
      }
    )
      .then(function () {
        textInput.value = "";
        return loadGuestbook();
      })
      .catch(function (error) {
        window.alert(
          "提交失败：" +
            error.message
        );
      })
      .finally(function () {
        if (
          !button.disabled ||
          button.textContent === "提交中…"
        ) {
          button.disabled = false;
        }

        button.textContent = "提交";
      });
  }

  byId("gb-send").addEventListener(
    "click",
    submitGuestbook
  );

  byId("gb-text").addEventListener(
    "keydown",
    function (event) {
      if (
        event.key === "Enter" &&
        !event.isComposing
      ) {
        event.preventDefault();
        submitGuestbook();
      }
    }
  );

  loadGuestbook().catch(function () {
    // 错误已被内部捕获
  });
})();
</script>
</body>
</html>`;

// -----------------------------------------------------------------------------
// HTTP 安全响应头
// -----------------------------------------------------------------------------

const COMMON_SECURITY_HEADERS: Readonly<
  Record<string, string>
> = {
  "x-content-type-options": "nosniff",
  "x-frame-options": "DENY",
  "referrer-policy":
    "strict-origin-when-cross-origin",
  "permissions-policy":
    "camera=(), microphone=(), geolocation=(), payment=(), usb=()",
  "cross-origin-opener-policy": "same-origin",
};

function withSecurityHeaders(
  response: Response,
): Response {
  if (response.status === 101) {
    return response;
  }

  const headers = new Headers(
    response.headers,
  );

  for (
    const [name, value] of
      Object.entries(COMMON_SECURITY_HEADERS)
  ) {
    if (!headers.has(name)) {
      headers.set(name, value);
    }
  }

  return new Response(
    response.body,
    {
      status: response.status,
      statusText: response.statusText,
      headers,
    },
  );
}

function htmlResponse(
  html: string,
): Response {
  return new Response(
    html,
    {
      headers: {
        "content-type":
          "text/html; charset=utf-8",
        "cache-control":
          "no-cache",
        "content-security-policy": [
          "default-src 'self'",
          "script-src 'self' 'unsafe-inline'",
          "style-src 'self' 'unsafe-inline'",
          "img-src 'self' data:",
          "font-src 'self'",
          "connect-src 'self' ws: wss:",
          "object-src 'none'",
          "base-uri 'none'",
          "form-action 'self'",
          "frame-ancestors 'none'",
        ].join("; "),
      },
    },
  );
}

// -----------------------------------------------------------------------------
// 运行时信息
// -----------------------------------------------------------------------------

function getRuntimeInfo() {
  return {
    app: APP_NAME,

    denoVersion:
      Deno.version.deno,

    v8Version:
      Deno.version.v8,

    tsVersion:
      Deno.version.typescript,

    region:
      Deno.env.get("DENO_REGION") ??
      "local",

    uptime:
      Math.round(
        (Date.now() - BOOT_TIME) /
          1000,
      ),

    deploymentId:
      Deno.env.get(
        "DENO_DEPLOYMENT_ID",
      ) ?? "playground",

    instanceId:
      INSTANCE_ID,

    kvApiAvailable,
    kvAvailable: kv !== null,
    kvError,

    localWebSocketConnections:
      clients.size,

    now:
      new Date().toISOString(),
  };
}

// -----------------------------------------------------------------------------
// 部署能力诊断
// -----------------------------------------------------------------------------

function getDiagnostics() {
  const runtime = Deno as typeof Deno & {
    upgradeWebSocket?: unknown;
    serve?: unknown;
  };

  const kvDiagnosis =
    kv !== null
      ? "Deno KV 已成功初始化。"
      : kvApiAvailable
      ? (
        "Deno.openKv 已存在，但数据库初始化失败。" +
        "请检查数据库关联状态、部署日志和数据库服务状态。"
      )
      : (
        "当前运行时没有暴露 Deno.openKv。" +
        "请确认数据库已连接到该 App，" +
        "并在关联数据库后创建一次全新 Deployment。"
      );

  return {
    app: APP_NAME,
    timestamp:
      new Date().toISOString(),

    runtime: {
      deno:
        Deno.version.deno,

      v8:
        Deno.version.v8,

      typescript:
        Deno.version.typescript,

      userAgent:
        navigator.userAgent,
    },

    deployment: {
      region:
        Deno.env.get(
          "DENO_REGION",
        ) ?? null,

      deploymentId:
        Deno.env.get(
          "DENO_DEPLOYMENT_ID",
        ) ?? null,

      instanceId:
        INSTANCE_ID,

      uptimeSeconds:
        Math.round(
          (Date.now() - BOOT_TIME) /
            1000,
        ),
    },

    capabilities: {
      denoServe:
        typeof runtime.serve ===
          "function",

      denoUpgradeWebSocket:
        typeof runtime
            .upgradeWebSocket ===
          "function",

      denoOpenKv:
        kvApiAvailable,

      readableStream:
        typeof ReadableStream ===
          "function",

      webCrypto:
        typeof crypto !==
          "undefined" &&
        typeof crypto.subtle
            ?.digest ===
          "function",

      randomUUID:
        typeof crypto !==
          "undefined" &&
        typeof crypto.randomUUID ===
          "function",
    },

    kv: {
      apiExposed:
        kvApiAvailable,

      connected:
        kv !== null,

      error:
        kvError,

      diagnosis:
        kvDiagnosis,
    },

    realtime: {
      localWebSocketConnections:
        clients.size,

      onlineCountScope:
        "current-instance",

      note:
        "使用 Deno KV Watch 机制全网广播点对点通信。",
    },

    limits: {
      maxHashBytes:
        MAX_HASH_BYTES,

      maxJsonBytes:
        MAX_JSON_BYTES,

      maxWebSocketFrameBytes:
        MAX_WS_FRAME_BYTES,

      maxGuestbookNameLength:
        MAX_GUESTBOOK_NAME_LENGTH,

      maxGuestbookTextLength:
        MAX_GUESTBOOK_TEXT_LENGTH,

      maxChatTextLength:
        MAX_CHAT_TEXT_LENGTH,
    },
  };
}

// -----------------------------------------------------------------------------
// 健康检查
// -----------------------------------------------------------------------------

function getHealthInfo() {
  return {
    ok: true,

    status:
      kv !== null
        ? "healthy"
        : "degraded",

    kvAvailable:
      kv !== null,

    kvApiAvailable,

    localWebSocketConnections:
      clients.size,

    uptimeSeconds:
      Math.round(
        (Date.now() - BOOT_TIME) /
          1000,
      ),

    instanceId:
      INSTANCE_ID,

    timestamp:
      new Date().toISOString(),
  };
}

// -----------------------------------------------------------------------------
// 主路由
// -----------------------------------------------------------------------------

async function handleRequest(
  req: Request,
): Promise<Response> {
  const url = new URL(req.url);
  const pathname = url.pathname;

  if (pathname === "/") {
    if (
      req.method !== "GET" &&
      req.method !== "HEAD"
    ) {
      return methodNotAllowed([
        "GET",
        "HEAD",
      ]);
    }

    if (req.method === "HEAD") {
      return new Response(
        null,
        {
          headers: {
            "content-type":
              "text/html; charset=utf-8",

            "cache-control":
              "no-cache",
          },
        },
      );
    }

    return htmlResponse(PAGE_HTML);
  }

  if (pathname === "/favicon.ico") {
    if (
      req.method !== "GET" &&
      req.method !== "HEAD"
    ) {
      return methodNotAllowed([
        "GET",
        "HEAD",
      ]);
    }

    return new Response(
      null,
      {
        status: 204,
        headers: {
          "cache-control":
            "public, max-age=86400",
        },
      },
    );
  }

  if (pathname === "/health") {
    if (
      req.method !== "GET" &&
      req.method !== "HEAD"
    ) {
      return methodNotAllowed([
        "GET",
        "HEAD",
      ]);
    }

    if (req.method === "HEAD") {
      return new Response(
        null,
        {
          status: 200,
          headers: {
            "cache-control":
              "no-store",
          },
        },
      );
    }

    return jsonResponse(
      getHealthInfo(),
    );
  }

  if (pathname === "/api/info") {
    if (req.method !== "GET") {
      return methodNotAllowed([
        "GET",
      ]);
    }

    return jsonResponse(
      getRuntimeInfo(),
    );
  }

  if (
    pathname ===
      "/api/diagnostics"
  ) {
    if (req.method !== "GET") {
      return methodNotAllowed([
        "GET",
      ]);
    }

    return jsonResponse(
      getDiagnostics(),
    );
  }

  if (pathname === "/api/visit") {
    if (req.method !== "POST") {
      return methodNotAllowed([
        "POST",
      ]);
    }

    checkRateLimit(
      req,
      "visit",
      30,
      60_000,
    );

    const count =
      await bumpVisits();

    if (count === null) {
      return jsonResponse(
        {
          count: null,

          error:
            kvApiAvailable
              ? (
                "Deno KV 初始化失败，" +
                "访问计数暂不可用"
              )
              : (
                "当前运行时未暴露 " +
                "Deno.openKv，" +
                "访问计数暂不可用"
              ),

          kvApiAvailable,
          kvError,
        },
        {
          status: 503,
        },
      );
    }

    return jsonResponse({
      count:
        count.toString(),
    });
  }

  if (pathname === "/api/hash") {
    if (req.method !== "POST") {
      return methodNotAllowed([
        "POST",
      ]);
    }

    checkRateLimit(
      req,
      "hash",
      60,
      60_000,
    );

    const text =
      await readTextBody(
        req,
        MAX_HASH_BYTES,
      );

    const encoded =
      new TextEncoder().encode(text);

    const digest =
      await crypto.subtle.digest(
        "SHA-256",
        encoded,
      );

    const sha256 =
      Array.from(
        new Uint8Array(digest),
      )
        .map((byte) => {
          return byte
            .toString(16)
            .padStart(2, "0");
        })
        .join("");

    return jsonResponse({
      algorithm: "SHA-256",
      bytes: encoded.byteLength,
      sha256,
    });
  }

  if (
    pathname === "/api/stream"
  ) {
    if (req.method !== "GET") {
      return methodNotAllowed([
        "GET",
      ]);
    }

    checkRateLimit(
      req,
      "stream",
      20,
      60_000,
    );

    return new Response(
      createSseStream(
        DEMO_TEXT,
        req.signal,
      ),
      {
        headers: {
          "content-type":
            "text/event-stream; charset=utf-8",

          "cache-control":
            "no-cache, no-transform",

          "x-accel-buffering":
            "no",
        },
      },
    );
  }

  if (
    pathname ===
      "/api/guestbook" &&
    req.method === "GET"
  ) {
    const rawLimit = Number(
      url.searchParams.get("limit") ??
        "20",
    );

    const limit =
      Number.isFinite(rawLimit)
        ? Math.max(
          1,
          Math.min(
            Math.floor(rawLimit),
            50,
          ),
        )
        : 20;

    const entries =
      await listGuestbook(limit);

    if (entries === null) {
      return jsonResponse(
        {
          error:
            kvApiAvailable
              ? (
                "Deno KV 初始化失败，" +
                "留言板暂不可用"
              )
              : (
                "当前运行时未暴露 " +
                "Deno.openKv，" +
                "留言板暂不可用"
              ),

          entries: [],
          kvApiAvailable,
          kvError,
        },
        {
          status: 503,
        },
      );
    }

    return jsonResponse(entries);
  }

  if (
    pathname ===
      "/api/guestbook" &&
    req.method === "POST"
  ) {
    checkRateLimit(
      req,
      "guestbook-write",
      5,
      60_000,
    );

    if (!kv) {
      return jsonResponse(
        {
          error:
            kvApiAvailable
              ? (
                "Deno KV 初始化失败，" +
                "无法保存留言"
              )
              : (
                "当前运行时未暴露 " +
                "Deno.openKv，" +
                "无法保存留言"
              ),

          kvApiAvailable,
          kvError,
        },
        {
          status: 503,
        },
      );
    }

    const body =
      await readJsonBody(
        req,
        MAX_JSON_BYTES,
      );

    if (
      typeof body !== "object" ||
      body === null ||
      Array.isArray(body)
    ) {
      throw new HttpError(
        400,
        "请求 JSON 必须是对象",
      );
    }

    const record =
      body as Record<
        string,
        unknown
      >;

    const name =
      normalizeText(
        record.name || "匿名",
        MAX_GUESTBOOK_NAME_LENGTH,
      ) || "匿名";

    const text =
      normalizeText(
        record.text,
        MAX_GUESTBOOK_TEXT_LENGTH,
      );

    if (!text) {
      throw new HttpError(
        400,
        "留言内容不能为空",
      );
    }

    const entry =
      await addGuestbookEntry(
        name,
        text,
      );

    if (entry === null) {
      return jsonResponse(
        {
          error:
            "Deno KV 不可用，无法保存留言",
        },
        {
          status: 503,
        },
      );
    }

    return jsonResponse(
      entry,
      {
        status: 201,
      },
    );
  }

  if (
    pathname ===
      "/api/guestbook"
  ) {
    return methodNotAllowed([
      "GET",
      "POST",
    ]);
  }

  if (pathname === "/ws") {
    if (req.method !== "GET") {
      return methodNotAllowed([
        "GET",
      ]);
    }

    return handleWebSocket(req);
  }

  return jsonResponse(
    {
      error: "Not Found",
      path: pathname,
    },
    {
      status: 404,
    },
  );
}

// -----------------------------------------------------------------------------
// 服务启动与统一错误处理
// -----------------------------------------------------------------------------

Deno.serve(
  async (
    req: Request,
  ): Promise<Response> => {
    try {
      const response =
        await handleRequest(req);

      return withSecurityHeaders(
        response,
      );
    } catch (error) {
      if (
        error instanceof HttpError
      ) {
        return withSecurityHeaders(
          jsonResponse(
            {
              error:
                error.message,
            },
            {
              status:
                error.status,

              headers:
                error.headers,
            },
          ),
        );
      }

      const requestId =
        crypto.randomUUID();

      console.error(
        "[Unhandled Error]",
        {
          requestId,
          method: req.method,
          url: req.url,
          error:
            getErrorMessage(error),

          stack:
            error instanceof Error
              ? error.stack
              : undefined,
        },
      );

      return withSecurityHeaders(
        jsonResponse(
          {
            error:
              "服务器内部错误",

            requestId,
          },
          {
            status: 500,
          },
        ),
      );
    }
  },
);

// ============================================================================
// FILE-END
// ============================================================================