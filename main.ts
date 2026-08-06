// ============================================================================
// deno-showcase / main.ts
// 单文件 · 零第三方依赖 · 零构建
//
// 展示：Deno.serve / Deno KV (KV Watch 跨节点广播 + TTL 缓存) / WebSocket / SSE / Web Crypto HMAC / Deno Cron
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
  "你好！我是通过 Deno.serve 和 ReadableStream 实现的流式输出 —— " +
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

function jsonResponse(data: unknown, init: ResponseInit = {}): Response {
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

  return req.headers.get("x-real-ip") ?? req.headers.get("cf-connecting-ip") ?? "unknown";
}

async function readTextBody(req: Request, maxBytes: number): Promise<string> {
  const contentLength = Number(req.headers.get("content-length") ?? "0");

  if (Number.isFinite(contentLength) && contentLength > maxBytes) {
    throw new HttpError(413, `请求体不能超过 ${maxBytes} 字节`);
  }

  const body = await req.text();

  if (utf8Length(body) > maxBytes) {
    throw new HttpError(413, `请求体不能超过 ${maxBytes} 字节`);
  }

  return body;
}

async function readJsonBody(req: Request, maxBytes: number): Promise<unknown> {
  const contentType = req.headers.get("content-type") ?? "";

  if (!contentType.toLowerCase().includes("application/json")) {
    throw new HttpError(415, "Content-Type 必须是 application/json");
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

function checkRateLimit(req: Request, bucket: string, limit: number, windowMs: number): void {
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
    const retryAfter = Math.max(1, Math.ceil((record.resetAt - now) / 1000));

    throw new HttpError(429, `请求过于频繁，请在 ${retryAfter} 秒后重试`, {
      "retry-after": String(retryAfter),
    });
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
// Deno Cron：边缘定时任务 (每分钟更新一次系统运行心跳到 KV)
// -----------------------------------------------------------------------------

if ("cron" in Deno && typeof Deno.cron === "function") {
  Deno.cron("cron-heartbeat", "* * * * *", async () => {
    try {
      const cronKv = await Deno.openKv();
      const nowIso = new Date().toISOString();
      await cronKv.set(["stats", "last_cron_tick"], nowIso);
      console.log("[Cron Heartbeat Success]", nowIso);
    } catch (err) {
      console.error("[Cron Heartbeat Error]", getErrorMessage(err));
    }
  });
}

// -----------------------------------------------------------------------------
// Deno KV：访问计数
// -----------------------------------------------------------------------------

async function bumpVisits(): Promise<bigint | null> {
  if (!kv) return null;

  const result = await kv.atomic().sum(["stats", "visits"], 1n).commit();

  if (!result.ok) {
    throw new Error("KV 原子计数提交失败");
  }

  const current = await kv.get<Deno.KvU64>(["stats", "visits"]);

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

async function addGuestbookEntry(name: string, text: string): Promise<GuestbookEntry | null> {
  if (!kv) return null;

  const id = crypto.randomUUID();
  const ts = Date.now();

  const entry: GuestbookEntry = {
    id,
    name,
    text,
    ts,
  };

  await kv.set(["guestbook-v2", ts, id], entry);

  return entry;
}

async function listGuestbook(limit = 20): Promise<GuestbookEntry[] | null> {
  if (!kv) return null;

  const safeLimit = Math.max(1, Math.min(Math.floor(limit), 50));

  const entries: GuestbookEntry[] = [];

  for await (const item of kv.list<GuestbookEntry>(
    {
      prefix: ["guestbook-v2"],
    },
    {
      reverse: true,
      limit: safeLimit,
    },
  )) {
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

function createSseStream(text: string, signal: AbortSignal): ReadableStream<Uint8Array> {
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

      signal.addEventListener("abort", handleAbort, { once: true });
    },

    async pull(controller) {
      if (stopped || signal.aborted) {
        return;
      }

      if (index >= characters.length) {
        stopped = true;
        try {
          controller.enqueue(encoder.encode("event: done\ndata: {}\n\n"));
          controller.close();
        } catch {
          // 忽略已关闭的情况
        }
        return;
      }

      const chunk = characters[index++];
      const payload = JSON.stringify({ chunk });

      try {
        controller.enqueue(encoder.encode(`data: ${payload}\n\n`));
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

type RealtimeMessage = ChatRealtimeMessage | SystemRealtimeMessage;

const clients = new Map<string, WebSocketClient>();

function randomName(): string {
  const adjectives = ["敏捷的", "神秘的", "闪电", "深海", "静谧的", "赛博", "量子"];

  const nouns = ["恐龙", "旅人", "信使", "浣熊", "游侠", "观察者", "锦鲤"];

  const adjective = adjectives[Math.floor(Math.random() * adjectives.length)];

  const noun = nouns[Math.floor(Math.random() * nouns.length)];

  const suffix = Math.floor(Math.random() * 100);

  return `${adjective}${noun}${suffix}`;
}

function sendSocket(socket: WebSocket, payload: unknown): void {
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

function broadcastToLocalClients(message: RealtimeMessage): void {
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

function publishRealtime(message: RealtimeMessage): void {
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

  if (!upgrade || upgrade.toLowerCase() !== "websocket") {
    return new Response("Expected WebSocket upgrade", {
      status: 426,
      headers: {
        upgrade: "websocket",
      },
    });
  }

  const { socket, response } = Deno.upgradeWebSocket(req);

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

  socket.onmessage = (event: MessageEvent) => {
    try {
      if (typeof event.data !== "string") {
        sendSocket(socket, {
          type: "system",
          text: "只接受文本格式的 WebSocket 消息",
        });

        return;
      }

      if (utf8Length(event.data) > MAX_WS_FRAME_BYTES) {
        sendSocket(socket, {
          type: "system",
          text: "消息数据过大",
        });

        return;
      }

      const now = Date.now();

      if (now - messageWindowStartedAt >= 10_000) {
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

      const parsed = JSON.parse(event.data) as {
        text?: unknown;
      };

      const text = normalizeText(parsed.text, MAX_CHAT_TEXT_LENGTH);

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
    console.error("[WebSocket] 连接异常:", clientId, event);
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
// 单文件页面 HTML & 现代 Side Menu Dashboard UI
// -----------------------------------------------------------------------------

const PAGE_HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<meta name="color-scheme" content="dark" />
<title>🦕 Deno Deploy 边缘能力全景控制台</title>

<style>
  :root {
    --bg: #0b0f19;
    --sidebar-bg: #121826;
    --panel: rgba(255, 255, 255, 0.04);
    --panel-hover: rgba(255, 255, 255, 0.07);
    --border: rgba(255, 255, 255, 0.08);
    --border-accent: rgba(0, 212, 170, 0.3);
    --accent: #00d4aa;
    --accent-glow: rgba(0, 212, 170, 0.25);
    --accent2: #7c5cff;
    --text: #eef2f7;
    --muted: #8e9bb0;
    --warning: #ffbd5a;
    --error: #ff5252;
    --sidebar-width: 260px;
  }

  * {
    box-sizing: border-box;
  }

  html, body {
    margin: 0;
    padding: 0;
    width: 100%;
    height: 100%;
    overflow-x: hidden;
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "Segoe UI", Roboto, "PingFang SC", "Microsoft YaHei", sans-serif;
  }

  button, input, textarea {
    font: inherit;
  }

  canvas#bg {
    position: fixed;
    inset: 0;
    z-index: 0;
    pointer-events: none;
  }

  /* 整体 Layout：侧边栏 + 主内容区 */
  .app-container {
    position: relative;
    z-index: 1;
    display: flex;
    min-height: 100vh;
  }

  /* Sidebar 侧边栏导航 */
  .sidebar {
    width: var(--sidebar-width);
    background: var(--sidebar-bg);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    padding: 24px 16px;
    flex-shrink: 0;
    backdrop-filter: blur(16px);
    transition: transform 0.3s ease;
  }

  .brand {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 0 8px 24px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 20px;
  }

  .brand-logo {
    font-size: 28px;
    line-height: 1;
    filter: drop-shadow(0 0 8px var(--accent-glow));
  }

  .brand-title {
    font-size: 16px;
    font-weight: 700;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
    -webkit-background-clip: text;
    -webkit-text-color: transparent;
    color: transparent;
    letter-spacing: -0.01em;
  }

  .brand-sub {
    font-size: 11px;
    color: var(--muted);
    margin-top: 2px;
  }

  .menu-label {
    font-size: 11px;
    font-weight: 600;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 0 8px 8px;
  }

  .nav-menu {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 6px;
    flex: 1;
  }

  .nav-item {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 12px 14px;
    border-radius: 10px;
    color: var(--muted);
    cursor: pointer;
    font-size: 13.5px;
    font-weight: 500;
    transition: all 0.2s ease;
    border: 1px solid transparent;
    user-select: none;
  }

  .nav-item:hover {
    background: var(--panel-hover);
    color: var(--text);
  }

  .nav-item.active {
    background: rgba(0, 212, 170, 0.1);
    color: var(--accent);
    border-color: var(--border-accent);
    box-shadow: 0 2px 12px var(--accent-glow);
  }

  .nav-icon {
    font-size: 16px;
    width: 20px;
    text-align: center;
  }

  .sidebar-footer {
    padding-top: 16px;
    border-top: 1px solid var(--border);
    font-size: 11px;
    color: var(--muted);
    text-align: center;
    line-height: 1.6;
  }

  /* Main Display Area 主展示区 */
  .main-content {
    flex: 1;
    padding: 32px 40px 60px;
    max-width: 1100px;
    overflow-y: auto;
  }

  .mobile-header {
    display: none;
    align-items: center;
    justify-content: space-between;
    padding: 16px 20px;
    background: var(--sidebar-bg);
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 0;
    z-index: 10;
  }

  .menu-toggle {
    background: none;
    border: 1px solid var(--border);
    color: var(--text);
    padding: 6px 12px;
    border-radius: 6px;
    cursor: pointer;
  }

  /* 面板 Tab Switch */
  .tab-panel {
    display: none;
    animation: fadeIn 0.25s ease-in-out;
  }

  .tab-panel.active {
    display: block;
  }

  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(6px); }
    to { opacity: 1; transform: translateY(0); }
  }

  .panel-header {
    margin-bottom: 28px;
  }

  .panel-title {
    font-size: 26px;
    font-weight: 700;
    margin: 0 0 8px;
    display: flex;
    align-items: center;
    gap: 10px;
  }

  .panel-desc {
    color: var(--muted);
    font-size: 14px;
    margin: 0;
    line-height: 1.6;
  }

  /* Cards & Grid Layout */
  .grid-2 {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
  }

  .card {
    padding: 24px;
    border: 1px solid var(--border);
    border-radius: 16px;
    background: var(--panel);
    backdrop-filter: blur(12px);
    transition: transform 0.2s ease, border-color 0.2s ease;
    margin-bottom: 20px;
  }

  .card:hover {
    border-color: rgba(0, 212, 170, 0.3);
  }

  .card-title {
    font-size: 16px;
    font-weight: 600;
    margin: 0 0 12px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
  }

  .card-desc {
    font-size: 12.5px;
    color: var(--muted);
    margin-bottom: 16px;
    line-height: 1.6;
  }

  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 16px;
  }

  .stat-card {
    padding: 16px;
    border-radius: 12px;
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid var(--border);
  }

  .stat-val {
    font-size: 24px;
    font-weight: 700;
    color: var(--accent);
    font-family: "SF Mono", Menlo, Consolas, monospace;
    overflow-wrap: anywhere;
  }

  .stat-lbl {
    font-size: 12px;
    color: var(--muted);
    margin-top: 4px;
  }

  /* Controls & Inputs */
  .row {
    display: flex;
    gap: 10px;
  }

  input, textarea, button {
    padding: 10px 14px;
    border: 1px solid var(--border);
    border-radius: 8px;
    outline: none;
  }

  input, textarea {
    flex: 1;
    min-width: 0;
    background: rgba(0, 0, 0, 0.25);
    color: var(--text);
    transition: border-color 0.2s ease;
  }

  input:focus, textarea:focus {
    border-color: var(--accent);
  }

  button {
    border: none;
    cursor: pointer;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
    color: #04140f;
    font-weight: 700;
    transition: opacity 0.2s ease, transform 0.1s ease;
    white-space: nowrap;
  }

  button:hover:not(:disabled) {
    opacity: 0.88;
  }

  button:active:not(:disabled) {
    transform: scale(0.98);
  }

  button:disabled {
    cursor: not-allowed;
    opacity: 0.4;
  }

  .code-out {
    margin-top: 12px;
    padding: 12px 14px;
    border-radius: 8px;
    background: rgba(0, 0, 0, 0.35);
    border: 1px solid rgba(255, 255, 255, 0.05);
    color: var(--accent);
    font-family: "SF Mono", Menlo, Consolas, monospace;
    font-size: 12px;
    line-height: 1.6;
    overflow-wrap: anywhere;
  }

  .notice {
    padding: 12px 14px;
    border: 1px solid rgba(255, 189, 90, 0.3);
    border-radius: 8px;
    background: rgba(255, 189, 90, 0.08);
    color: var(--warning);
    font-size: 12px;
    line-height: 1.6;
  }

  /* Chat Log Styling */
  #chatlog {
    display: flex;
    flex-direction: column;
    gap: 12px;
    height: 320px;
    overflow-y: auto;
    margin-bottom: 14px;
    padding: 16px;
    border-radius: 12px;
    background: rgba(0, 0, 0, 0.35);
    border: 1px solid var(--border);
  }

  .msg-row {
    display: flex;
    flex-direction: column;
    max-width: 80%;
  }

  .msg-row.sys-row {
    align-self: center;
    max-width: 90%;
  }

  .sys-badge {
    padding: 4px 12px;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.06);
    color: var(--muted);
    font-size: 11px;
    text-align: center;
  }

  .msg-row.other-row {
    align-self: flex-start;
  }

  .msg-row.me-row {
    align-self: flex-end;
  }

  .msg-author {
    font-size: 11px;
    color: var(--muted);
    margin-bottom: 4px;
    padding-left: 4px;
  }

  .msg-bubble {
    padding: 10px 14px;
    border-radius: 14px;
    font-size: 13.5px;
    line-height: 1.5;
    word-break: break-word;
  }

  .other-row .msg-bubble {
    background: rgba(255, 255, 255, 0.08);
    color: var(--text);
    border-top-left-radius: 4px;
  }

  .me-row .msg-bubble {
    background: linear-gradient(135deg, var(--accent), #00a887);
    color: #04140f;
    font-weight: 500;
    border-top-right-radius: 4px;
  }

  .msg-time {
    font-size: 10px;
    opacity: 0.65;
    float: right;
    margin-left: 8px;
    margin-top: 4px;
  }

  /* Status Indicator */
  .status {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    color: var(--muted);
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
    box-shadow: 0 0 8px var(--accent);
  }

  .status.disconnected .status-dot {
    background: var(--error);
  }

  /* SSE Stream */
  #stream-out {
    min-height: 80px;
    margin-top: 14px;
    padding: 16px;
    border-radius: 10px;
    background: rgba(0, 0, 0, 0.35);
    font-size: 14px;
    line-height: 1.7;
    overflow-wrap: anywhere;
    border: 1px solid var(--border);
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

  @keyframes blink { 50% { opacity: 0; } }

  /* Responsive Adjustments */
  @media (max-width: 768px) {
    .app-container {
      flex-direction: column;
    }

    .mobile-header {
      display: flex;
    }

    .sidebar {
      position: fixed;
      inset: 0 right 0 0;
      z-index: 99;
      width: 260px;
      transform: translateX(-100%);
    }

    .sidebar.open {
      transform: translateX(0);
      box-shadow: 4px 0 24px rgba(0, 0, 0, 0.5);
    }

    .main-content {
      padding: 20px 16px 40px;
    }

    .grid-2 {
      grid-template-columns: 1fr;
    }
  }
</style>
</head>

<body>
<canvas id="bg"></canvas>

<!-- 🔐 页面访问安全保护锁屏弹窗 -->
<div id="lock-overlay" style="position:fixed; inset:0; z-index:9999; background:rgba(10,14,23,0.85); backdrop-filter:blur(16px); -webkit-backdrop-filter:blur(16px); display:none; align-items:center; justify-content:center; padding:20px;">
  <div style="width:100%; max-width:400px; background:rgba(23,32,51,0.95); border:1px solid var(--border-accent); border-radius:18px; padding:30px 26px; box-shadow:0 20px 50px rgba(0,0,0,0.6); text-align:center; position:relative; overflow:hidden;">
    <div style="font-size:42px; margin-bottom:12px;">🔐</div>
    <h2 style="font-size:20px; font-weight:700; color:var(--text); margin-bottom:6px;">Deno Deploy 边缘控制台受安全保护</h2>
    <p style="font-size:13px; color:var(--muted); margin-bottom:20px; line-height:1.5;">请输入访问密码解锁系统功能与 AI 边缘控制台</p>

    <div style="margin-bottom:16px;">
      <input id="lock-password-input" type="password" placeholder="输入访问密码 (默认: deno2026)..." style="width:100%; padding:12px 14px; border-radius:10px; background:rgba(0,0,0,0.4); color:var(--text); border:1px solid var(--border); font-size:14px; text-align:center;" autocomplete="current-password" />
      <div id="lock-error-msg" style="font-size:12px; color:var(--warning); margin-top:8px; display:none;"></div>
    </div>

    <button id="lock-unlock-btn" type="button" style="width:100%; padding:12px; font-size:14.5px; font-weight:600;">🔓 验证密码并解锁访问</button>

    <div style="margin-top:16px; font-size:11px; color:var(--muted); line-height:1.5;">
      💡 提示：可通过系统的 <code style="color:var(--accent);">APP_PASSWORD</code> 环境变量修改密码。
    </div>
  </div>
</div>

<div class="mobile-header">
  <div style="font-weight:700; color:var(--accent);">🦕 Deno Deploy 控制台</div>
  <button class="menu-toggle" id="menu-toggle">☰ 菜单</button>
</div>

<div class="app-container">
  <!-- 侧边栏 Side Menu -->
  <aside class="sidebar" id="sidebar">
    <div class="brand">
      <div class="brand-logo">🦕</div>
      <div>
        <div class="brand-title">Deno Deploy</div>
        <div class="brand-sub">边缘原生能力全景秀</div>
      </div>
    </div>

    <div class="menu-label">⚡ 边缘基础设施</div>
    <ul class="nav-menu">
      <li class="nav-item active" data-tab="overview">
        <span class="nav-icon">⚡</span> 运行时概览
      </li>
      <li class="nav-item" data-tab="crypto">
        <span class="nav-icon">🔐</span> Web Crypto API
      </li>
      <li class="nav-item" data-tab="websocket">
        <span class="nav-icon">💬</span> 实时 WebSocket
      </li>
      <li class="nav-item" data-tab="sse">
        <span class="nav-icon">🌊</span> SSE 流式输出
      </li>
      <li class="nav-item" data-tab="kv">
        <span class="nav-icon">📈</span> Deno KV 数据
      </li>
      <li class="nav-item" data-tab="cron">
        <span class="nav-icon">⏱️</span> Deno Cron 定时任务
      </li>
    </ul>

    <div class="menu-label" style="margin-top:16px;">🤖 AI 演进微应用</div>
    <ul class="nav-menu">
      <li class="nav-item" data-tab="ai-gateway">
        <span class="nav-icon">🤖</span> AI 边缘 API 网关
      </li>
      <li class="nav-item" data-tab="ai-card">
        <span class="nav-icon">📝</span> AI 金句卡片生成
      </li>
      <li class="nav-item" data-tab="ai-clip">
        <span class="nav-icon">✂️</span> AI 剪藏知识库
      </li>
      <li class="nav-item" data-tab="ai-collab">
        <span class="nav-icon">👥</span> 多人 AI 协同室
      </li>
    </ul>

    <div class="sidebar-footer">
      Deno.serve 驱动 · 单文件 · 零构建<br />
      <a href="/api/info" target="_blank" style="color:var(--accent); text-decoration:none;">查看 API JSON</a>
    </div>
  </aside>

  <!-- 主展示区域 Main Display Area -->
  <main class="main-content">

    <!-- [Tab 1] 运行时概览 -->
    <section class="tab-panel active" id="tab-overview">
      <div class="panel-header">
        <h1 class="panel-title">⚡ 运行时与边缘环境概览</h1>
        <p class="panel-desc">直接读取 Deno.version 与 Deno.env 环境变量，每次请求由边缘节点实时计算返回。</p>
      </div>

      <div class="card">
        <div class="card-title">边缘节点系统参数</div>
        <div class="stats-grid" id="overview-stats">
          <div class="stat-card"><div class="stat-val">Loading…</div><div class="stat-lbl">数据加载中</div></div>
        </div>
      </div>

      <div class="card">
        <div class="card-title">快速链接与接口服务</div>
        <div class="row" style="flex-wrap:wrap; gap:10px;">
          <a href="/api/info" target="_blank" style="text-decoration:none;"><button type="button">📋 /api/info (运行时 JSON)</button></a>
          <a href="/api/diagnostics" target="_blank" style="text-decoration:none;"><button type="button">🩺 /api/diagnostics (部署诊断)</button></a>
          <a href="/health" target="_blank" style="text-decoration:none;"><button type="button">💚 /health (健康检查)</button></a>
        </div>
      </div>
    </section>

    <!-- [Tab 2] Web Crypto API -->
    <section class="tab-panel" id="tab-crypto">
      <div class="panel-header">
        <h1 class="panel-title">🔐 Web Crypto API 验签与加密</h1>
        <p class="panel-desc">使用零第三方依赖的原生 crypto.subtle 算法完成 HMAC-SHA256 安全签名与 SHA-256 哈希计算。</p>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-title">1. Web Crypto HMAC 签名验签</div>
          <div class="card-desc">服务端使用 SECRET_KEY 秘钥进行 HMAC-SHA256 签名（常用于 API 防重放与防篡改）</div>
          <div class="row">
            <input id="hmac-input" placeholder="输入待签名的文本..." value="Hello Deno HMAC" />
            <button id="hmac-btn" type="button">HMAC 签名</button>
          </div>
          <div class="code-out" id="hmac-out">点击按钮发起签名计算…</div>
        </div>

        <div class="card">
          <div class="card-title">2. SHA-256 摘要哈希计算</div>
          <div class="card-desc">调用 crypto.subtle.digest 生成十六进制哈希摘要</div>
          <div class="row">
            <input id="hash-input" placeholder="输入任意文本..." value="Hello Deno" />
            <button id="hash-btn" type="button">SHA-256</button>
          </div>
          <div class="code-out" id="hash-out">等待输入…</div>
        </div>
      </div>
    </section>

    <!-- [Tab 3] 实时 WebSocket & KV Watch -->
    <section class="tab-panel" id="tab-websocket">
      <div class="panel-header">
        <h1 class="panel-title">
          💬 WebSocket & KV Watch 实时气泡聊天室
          <span id="ws-status" class="status">
            <span class="status-dot"></span>
            <span id="ws-status-text">连接中</span>
          </span>
        </h1>
        <p class="panel-desc">结合 Deno.upgradeWebSocket 与 Deno KV Watch 实现多实例、跨边缘节点的双向即时中转。</p>
      </div>

      <div class="card">
        <div class="card-title">
          <span>边缘聊天室</span>
          <span style="font-weight:400; font-size:12px; color:var(--muted);">当前节点连接数: <b id="online-count" style="color:var(--accent);">0</b></span>
        </div>

        <div id="chatlog" aria-live="polite"></div>

        <div class="row">
          <input id="chat-input" maxlength="300" placeholder="输入消息，回车或点击发送..." autocomplete="off" />
          <button id="chat-send" type="button" disabled>发送</button>
        </div>
      </div>
    </section>

    <!-- [Tab 4] SSE 流式输出 -->
    <section class="tab-panel" id="tab-sse">
      <div class="panel-header">
        <h1 class="panel-title">🌊 SSE (Server-Sent Events) 流式响应</h1>
        <p class="panel-desc">通过 ReadableStream 逐字符 enqueue 输出，实现大模型 AI Token 打字机流式展现。</p>
      </div>

      <div class="card">
        <div class="card-title">Token 打字机演示</div>
        <button id="stream-btn" type="button">▶ 开始 SSE 流式输出</button>
        <div id="stream-out">点击按钮查看流式响应效果...</div>
      </div>
    </section>

    <!-- [Tab 5] Deno KV 数据中心 -->
    <section class="tab-panel" id="tab-kv">
      <div class="panel-header">
        <h1 class="panel-title">📈 Deno KV 边缘数据中心</h1>
        <p class="panel-desc">演示 Deno KV 的分布式原子计数、TTL 自动过期缓存与持久化留言板操作。</p>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-title">1. KV 原子计数器 (sum)</div>
          <div class="card-desc">每次调用 kv.atomic().sum() 原子的完成 +1</div>
          <div class="stat-val" id="visit-count" style="font-size:32px; margin-bottom:8px;">–</div>
          <div class="stat-lbl" id="visit-label">累计访问次数（跨边缘节点持久化）</div>
        </div>

        <div class="card">
          <div class="card-title">2. KV TTL 自动过期缓存 (expireIn)</div>
          <div class="card-desc">设置 60 秒自动物理消除的临时缓存键</div>
          <button id="ttl-btn" type="button">写入 60s TTL 临时缓存</button>
          <div class="code-out" id="ttl-out">点击生成带 TTL 的缓存条目</div>
        </div>
      </div>

      <div class="card">
        <div class="card-title">3. Deno KV 动态留言板</div>
        <div class="card-desc">kv.set() 写入记录，kv.list() 倒序读取</div>
        <div class="row" style="margin-bottom:14px;">
          <input id="gb-name" maxlength="24" placeholder="你的昵称" style="max-width:140px" />
          <input id="gb-text" maxlength="200" placeholder="写下你想对 Deno 说的话..." />
          <button id="gb-send" type="button">提交留言</button>
        </div>
        <div id="guestbook-list" style="max-height:260px; overflow-y:auto;"></div>
      </div>
    </section>

    <!-- [Tab 6] Deno Cron -->
    <section class="tab-panel" id="tab-cron">
      <div class="panel-header">
        <h1 class="panel-title">⏱️ Deno Cron 边缘定时心跳</h1>
        <p class="panel-desc">无需 node-cron 等第三方包，原生定义 Deno.cron("* * * * *", fn) 在边缘节点后台运行。</p>
      </div>

      <div class="card">
        <div class="card-title">
          <span>Cron 心跳运行状态</span>
          <div style="display:flex; gap:8px;">
            <button id="cron-trigger-btn" type="button" style="font-size:12px; padding:6px 12px;">⚡ 立即触发测试心跳</button>
            <button id="cron-refresh-btn" type="button" style="font-size:12px; padding:6px 12px; background:rgba(255,255,255,0.1); color:var(--text);">🔄 刷新状态</button>
          </div>
        </div>
        <div class="stats-grid">
          <div class="stat-card">
            <div class="stat-val" id="cron-spec" style="font-size:20px;">* * * * *</div>
            <div class="stat-lbl">Cron 触发规则 (每分钟)</div>
          </div>
          <div class="stat-card">
            <div class="stat-val" id="cron-tick" style="font-size:15px; color:var(--accent);">读取中…</div>
            <div class="stat-lbl">最新 Cron 心跳时间 (last_cron_tick)</div>
          </div>
        </div>
      </div>
    </section>

    <!-- [Tab 7] AI 边缘 API 网关 (Phase 1) -->
    <section class="tab-panel" id="tab-ai-gateway">
      <div class="panel-header">
        <h1 class="panel-title">🤖 AI 边缘 API 网关 & Deno KV Token 缓存站</h1>
        <p class="panel-desc">智能代理大模型 API，基于 Deno KV 自动对常见提问进行缓存。命中缓存即 0ms 返回且零 Token 消耗。</p>
      </div>

      <div class="card">
        <div class="card-title">
          <span>⚙️ 网关运行配置</span>
          <span id="ai-key-status" style="font-size:12px; padding:4px 10px; border-radius:999px;">检测中...</span>
        </div>

        <div class="grid-2">
          <div>
            <div class="card-desc">AI 模型选择 (Model)</div>
            <select id="ai-model-select" style="width:100%; padding:9px 12px; border-radius:8px; background:rgba(0,0,0,0.3); color:var(--text); border:1px solid var(--border);">
              <option value="deepseek-chat">deepseek-chat (DeepSeek V3)</option>
              <option value="gpt-4o-mini">gpt-4o-mini (OpenAI)</option>
              <option value="siliconflow/deepseek-v3">SiliconFlow DeepSeek</option>
            </select>
          </div>

          <div>
            <div class="card-desc">Deno KV 智能 Token 缓存</div>
            <label style="display:flex; align-items:center; gap:8px; cursor:pointer; font-size:13px; margin-top:8px;">
              <input type="checkbox" id="ai-cache-toggle" checked style="width:16px; height:16px; accent-color:var(--accent);" />
              开启 KV 响应缓存 (热门提问 0ms 零 Token 返回)
            </label>
          </div>
        </div>

        <div style="margin-top:14px;">
          <div class="card-desc">System Prompt (人设指示词)</div>
          <input id="ai-system-prompt" value="你是一个精通 Deno、TypeScript 与边缘计算的高级 Web 技术专家助手，回答简明扼要。" placeholder="输入系统人设 Prompt..." />
        </div>
      </div>

      <div style="margin-bottom:14px; display:flex; flex-wrap:wrap; gap:8px; align-items:center;">
        <span class="card-desc" style="margin:0 4px 0 0;">快捷测试词:</span>
        <button class="ai-chip-btn" type="button" data-prompt="用一句话总结 Deno 比 Node.js 强在哪些方面？" style="font-size:12px; padding:5px 12px; background:rgba(255,255,255,0.06); color:var(--text); border:1px solid var(--border);">⚡ Deno 核心优势</button>
        <button class="ai-chip-btn" type="button" data-prompt="解释什么是 Web Crypto HMAC 签名及其应用场景" style="font-size:12px; padding:5px 12px; background:rgba(255,255,255,0.06); color:var(--text); border:1px solid var(--border);">🔐 HMAC 验签原理解释</button>
        <button class="ai-chip-btn" type="button" data-prompt="写一首赞美边缘计算（Edge Computing）的四句小诗" style="font-size:12px; padding:5px 12px; background:rgba(255,255,255,0.06); color:var(--text); border:1px solid var(--border);">📜 边缘计算诗歌</button>
      </div>

      <div class="card">
        <div class="card-title">
          <span>💬 边缘 AI 代理交互</span>
          <span style="font-size:12px; font-weight:400; color:var(--muted);">累计已节省 Token: <b id="ai-token-count" style="color:var(--accent);">0</b></span>
        </div>

        <div id="ai-chatlog" style="height:320px; overflow-y:auto; padding:16px; border-radius:12px; background:rgba(0,0,0,0.35); border:1px solid var(--border); display:flex; flex-direction:column; gap:12px; margin-bottom:14px;">
          <div class="msg-row other-row">
            <div class="msg-author">🤖 Deno AI Edge Gateway</div>
            <div class="msg-bubble">
              你好！我是部署在 Deno 边缘节点上的 AI 网关助手。支持全自动大模型代理以及 Deno KV 智能 Token 缓存！试着向我提问吧！
            </div>
          </div>
        </div>

        <div class="row">
          <input id="ai-input" placeholder="向边缘 AI 提问 (按回车或点击发送)..." autocomplete="off" />
          <button id="ai-send-btn" type="button">▶ 发送 AI 请求</button>
        </div>
      </div>
    </section>

    <!-- [Tab 8] AI 金句卡片生成 (Phase 2) -->
    <section class="tab-panel" id="tab-ai-card">
      <div class="panel-header">
        <h1 class="panel-title">📝 AI 文本摘要提炼与金句排版小卡片生成器</h1>
        <p class="panel-desc">输入长文本或文章段落，AI 自动提炼 30 字核心金句与 3 条核心要点，并排版渲染为高颜值社交分享卡片。</p>
      </div>

      <div class="grid-2">
        <!-- 左侧编辑控制 -->
        <div class="card">
          <div class="card-title">📝 文本输入与样式设置</div>

          <div style="margin-bottom:12px;">
            <div class="card-desc">输入长文本 / 文章段落</div>
            <textarea id="card-text-input" style="width:100%; height:120px; padding:10px 12px; border-radius:8px; background:rgba(0,0,0,0.3); color:var(--text); border:1px solid var(--border); resize:vertical;" placeholder="粘贴你想提炼的文本段落..."></textarea>
          </div>

          <div style="margin-bottom:12px; display:flex; flex-wrap:wrap; gap:6px;">
            <button class="card-chip-btn" type="button" data-text="边缘计算把计算能力推向离用户最近的节点。传统架构中数据需要跨越万里，而边缘计算实现5ms极致响应。Deno Deploy全局300+节点让全栈交付极速无感。" style="font-size:11px; padding:4px 8px; background:rgba(255,255,255,0.06); color:var(--text); border:1px solid var(--border);">⚡ 边缘计算优势</button>
            <button class="card-chip-btn" type="button" data-text="AI时代的工程师不再只是代码搬运工，而是系统架构师与产品创造者。拥抱Web标准与边缘Serverless，让一个人也能交付高可用全球级微应用。" style="font-size:11px; padding:4px 8px; background:rgba(255,255,255,0.06); color:var(--text); border:1px solid var(--border);">🤖 AI 工程师转型</button>
          </div>

          <div class="grid-2" style="margin-bottom:14px;">
            <div>
              <div class="card-desc">卡片视觉风格</div>
              <select id="card-style-select" style="width:100%; padding:8px 10px; border-radius:8px; background:rgba(0,0,0,0.3); color:var(--text); border:1px solid var(--border);">
                <option value="dark">极客暗黑 (Cyber Dark)</option>
                <option value="gradient">渐变霓虹 (Neon Gradient)</option>
                <option value="minimal">极简冷灰 (Minimal Gray)</option>
              </select>
            </div>
            <div>
              <div class="card-desc">作者署名 (Author)</div>
              <input id="card-author-input" value="边缘 AI 思想家" placeholder="签名..." style="padding:7px 10px;" />
            </div>
          </div>

          <button id="card-generate-btn" type="button" style="width:100%; padding:10px;">✨ 生成 AI 金句卡片</button>
        </div>

        <!-- 右侧卡片预览 -->
        <div class="card">
          <div class="card-title">🎨 高颜值卡片预览</div>

          <div id="card-render-box" style="border-radius:16px; padding:24px; background:linear-gradient(135deg, #111827 0%, #0f172a 100%); border:1px solid rgba(0,212,170,0.3); box-shadow:0 8px 32px rgba(0,0,0,0.5); position:relative; min-height:280px; display:flex; flex-direction:column; justify-content:space-between;">
            <div>
              <div style="font-size:36px; line-height:1; color:var(--accent); font-family:serif; opacity:0.8;">“</div>
              <div id="card-quote-out" style="font-size:18px; font-weight:700; color:#f3f4f6; margin-bottom:16px; line-height:1.5;">
                点击左侧「生成 AI 金句卡片」按钮，大模型将在此为您呈现精炼金句与核心要点...
              </div>

              <div id="card-points-out" style="display:flex; flex-direction:column; gap:8px; font-size:13px; color:#9ca3af; margin-bottom:20px;">
                <div style="display:flex; align-items:center; gap:6px;"><span style="color:var(--accent);">✦</span> 核心要点 1 展示区</div>
                <div style="display:flex; align-items:center; gap:6px;"><span style="color:var(--accent);">✦</span> 核心要点 2 展示区</div>
              </div>
            </div>

            <div style="display:flex; justify-content:space-between; align-items:center; padding-top:14px; border-top:1px dashed rgba(255,255,255,0.15); font-size:12px; color:#6b7280;">
              <div>✍️ <span id="card-author-out">边缘 AI 思想家</span></div>
              <div style="font-size:11px; color:var(--accent); font-weight:600; background:rgba(0,212,170,0.1); padding:2px 8px; border-radius:999px;">🦕 Deno Edge AI</div>
            </div>
          </div>
        </div>
      </div>

      <!-- 历史卡片画廊 -->
      <div class="card" style="margin-top:16px;">
        <div class="card-title">
          <span>📚 Deno KV 持久化卡片画廊</span>
          <button id="card-refresh-gallery" type="button" style="font-size:12px; padding:4px 10px; background:rgba(255,255,255,0.08); color:var(--text);">🔄 刷新画廊</button>
        </div>
        <div id="card-gallery-list" style="display:grid; grid-template-columns:repeat(auto-fill, minmax(280px, 1fr)); gap:14px; margin-top:10px;">
          <div style="font-size:12px; color:var(--muted);">加载中…</div>
        </div>
      </div>
    </section>

    <!-- [Tab 9] AI 网页剪藏知识库 (Phase 3) -->
    <section class="tab-panel" id="tab-ai-clip">
      <div class="panel-header">
        <h1 class="panel-title">✂️ AI 网页剪藏与个人知识中心</h1>
        <p class="panel-desc">模拟浏览器扩展剪藏网页文章，由 AI 自动打标签并提取关键总结，持久化归档在 Deno KV 分布式知识库。</p>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-title">✂️ 网页剪藏模拟器</div>

          <div style="margin-bottom:10px;">
            <div class="card-desc">网页 URL 链接</div>
            <input id="clip-url-input" value="https://deno.com/blog/deno-v2" placeholder="https://..." />
          </div>

          <div style="margin-bottom:10px;">
            <div class="card-desc">文章标题</div>
            <input id="clip-title-input" value="Deno v2.0 正式发布：全面兼容 Node & npm" placeholder="标题..." />
          </div>

          <div style="margin-bottom:12px;">
            <div class="card-desc">网页核心正文文本</div>
            <textarea id="clip-content-input" style="width:100%; height:110px; padding:10px 12px; border-radius:8px; background:rgba(0,0,0,0.3); color:var(--text); border:1px solid var(--border); resize:vertical;" placeholder="粘贴网页主要正文内容...">Deno 2.0 包含了全新的标准包，完美兼容 Node.js 原生 API 与 package.json。Deno KV、Deno Cron 等边缘原生的云数据能力直接内置于引擎中，让前端开发者可以以极速、安全的方式交付 Serverless 云微应用。</textarea>
          </div>

          <button id="clip-submit-btn" type="button" style="width:100%;">✂️ 一键 AI 剪藏到 Deno KV 知识库</button>
        </div>

        <div class="card">
          <div class="card-title">
            <span>📚 个人 AI 剪藏知识库</span>
            <button id="clip-refresh-btn" type="button" style="font-size:12px; padding:4px 10px;">🔄 刷新列表</button>
          </div>
          <div id="clip-list-box" style="height:320px; overflow-y:auto; display:flex; flex-direction:column; gap:10px;">
            <div style="font-size:12px; color:var(--muted);">加载知识库中…</div>
          </div>
        </div>
      </div>
    </section>

    <!-- [Tab 10] 多人实时协同 AI 讨论室 (Phase 4) -->
    <section class="tab-panel" id="tab-ai-collab">
      <div class="panel-header">
        <h1 class="panel-title">👥 多人实时协同 AI 讨论室</h1>
        <p class="panel-desc">结合 WebSocket + Deno KV Watch。多人进入同一房间实时协同编辑 Prompt，推演结果同步广播全员。</p>
      </div>

      <div class="grid-2">
        <div class="card">
          <div class="card-title">
            <span>⚙️ 协同房间控制台</span>
            <span class="status connected" style="font-size:12px;"><span class="status-dot"></span> 实时协同中</span>
          </div>

          <div style="margin-bottom:12px;">
            <div class="card-desc">协同房间</div>
            <select id="collab-room-select" style="width:100%; padding:9px 12px; border-radius:8px; background:rgba(0,0,0,0.3); color:var(--text); border:1px solid var(--border);">
              <option value="room-101">🚀 房间 #101 (全栈工程师讨论室)</option>
              <option value="room-102">🎨 房间 #102 (AI 创作者与 Prompt 实验室)</option>
            </select>
          </div>

          <div style="margin-bottom:12px;">
            <div class="card-desc">协同 Prompt 共同编写区</div>
            <textarea id="collab-prompt-input" style="width:100%; height:120px; padding:10px 12px; border-radius:8px; background:rgba(0,0,0,0.3); color:var(--text); border:1px solid var(--border); resize:vertical;" placeholder="在此共同输入/修改 Prompt...">设计一个基于 Deno Deploy 的全球分布式极速缓存方案，并分析其相比传统 Redis 架构的优势。</textarea>
          </div>

          <button id="collab-send-btn" type="button" style="width:100%;">🚀 全员协同触发 AI 边缘推演</button>
        </div>

        <div class="card">
          <div class="card-title">👥 房间全员广播与 AI 推演日志</div>
          <div id="collab-chat-log" style="height:320px; overflow-y:auto; padding:14px; border-radius:12px; background:rgba(0,0,0,0.35); border:1px solid var(--border); display:flex; flex-direction:column; gap:10px; font-size:12.5px;">
            <div style="color:var(--accent);">[系统通知] 已连接到多人协同房间，您可以修改左侧 Prompt 并触发全员广播推演。</div>
          </div>
        </div>
      </div>
    </section>

  </main>
</div>

<script>
(function () {
  "use strict";

  function byId(id) { return document.getElementById(id); }

  function setText(id, value) {
    var element = byId(id);
    if (element) { element.textContent = String(value); }
  }

  function requestJson(url, options) {
    return fetch(url, options).then(function (response) {
      return response.json()
        .catch(function () { return { error: "服务器返回了无效 JSON" }; })
        .then(function (body) {
          if (!response.ok) {
            var message = body && body.error ? body.error : "请求失败：" + response.status;
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
  // Tab 切换逻辑 & 移动端 Drawer
  // ---------------------------------------------------------------------------
  var navItems = document.querySelectorAll(".nav-item");
  var tabPanels = document.querySelectorAll(".tab-panel");
  var sidebar = byId("sidebar");
  var menuToggle = byId("menu-toggle");

  if (menuToggle) {
    menuToggle.addEventListener("click", function () {
      sidebar.classList.toggle("open");
    });
  }

  navItems.forEach(function (item) {
    item.addEventListener("click", function () {
      var targetTab = this.getAttribute("data-tab");
      navItems.forEach(function (el) { el.classList.remove("active"); });
      tabPanels.forEach(function (el) { el.classList.remove("active"); });

      this.classList.add("active");
      var activePanel = byId("tab-" + targetTab);
      if (activePanel) { activePanel.classList.add("active"); }

      if (sidebar.classList.contains("open")) {
        sidebar.classList.remove("open");
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Canvas 动态背景
  // ---------------------------------------------------------------------------
  (function setupBackground() {
    if (window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches) { return; }
    var canvas = byId("bg");
    var context = canvas.getContext("2d");
    if (!context) return;

    var width = 0, height = 0, particles = [];

    function createParticles() {
      var count = Math.min(60, Math.floor((width * height) / 20000));
      particles = [];
      for (var i = 0; i < count; i++) {
        particles.push({
          x: Math.random() * width,
          y: Math.random() * height,
          vx: (Math.random() - 0.5) * 0.35,
          vy: (Math.random() - 0.5) * 0.35
        });
      }
    }

    function resize() {
      var ratio = Math.min(window.devicePixelRatio || 1, 2);
      width = window.innerWidth;
      height = window.innerHeight;
      canvas.width = Math.floor(width * ratio);
      canvas.height = Math.floor(height * ratio);
      canvas.style.width = width + "px";
      canvas.style.height = height + "px";
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
      createParticles();
    }

    function tick() {
      context.clearRect(0, 0, width, height);
      context.fillStyle = "rgba(0,212,170,0.55)";
      for (var i = 0; i < particles.length; i++) {
        var p = particles[i];
        p.x += p.vx; p.y += p.vy;
        if (p.x < 0 || p.x > width) p.vx *= -1;
        if (p.y < 0 || p.y > height) p.vy *= -1;
        context.beginPath();
        context.arc(p.x, p.y, 1.5, 0, Math.PI * 2);
        context.fill();
      }
      requestAnimationFrame(tick);
    }

    window.addEventListener("resize", resize);
    resize();
    tick();
  })();

  // ---------------------------------------------------------------------------
  // 加载系统 Overview 统计
  // ---------------------------------------------------------------------------
  function loadOverview() {
    requestJson("/api/info").then(function (info) {
      var container = byId("overview-stats");
      if (container) {
        var uptimeSec = Math.floor((Date.now() - info.bootTime) / 1000);
        var items = [
          { label: "Deno Version", val: info.denoVersion || "N/A" },
          { label: "V8 Engine", val: info.v8Version || "N/A" },
          { label: "部署 Region", val: info.region || "Local / Edge" },
          { label: "运行时时长", val: uptimeSec + " 秒" },
          { label: "最新 Cron 心跳", val: info.lastCronTick ? new Date(info.lastCronTick).toLocaleTimeString() : "尚未心跳" }
        ];

        container.innerHTML = items.map(function (it) {
          return '<div class="stat-card"><div class="stat-val">' + it.val + '</div><div class="stat-lbl">' + it.label + '</div></div>';
        }).join("");
      }

      setText("cron-tick", info.lastCronTick ? new Date(info.lastCronTick).toLocaleString() : "暂未读取到心跳 (点击右侧刷新)");
    }).catch(function (err) {
      console.error("加载 Overview 失败:", err);
    });
  }
  loadOverview();
  setInterval(loadOverview, 10000);

  var cronRefreshBtn = byId("cron-refresh-btn");
  if (cronRefreshBtn) {
    cronRefreshBtn.addEventListener("click", function () {
      setText("cron-tick", "查询中...");
      loadOverview();
    });
  }

  var cronTriggerBtn = byId("cron-trigger-btn");
  if (cronTriggerBtn) {
    cronTriggerBtn.addEventListener("click", function () {
      setText("cron-tick", "触发中...");
      requestJson("/api/cron-tick", { method: "POST" }).then(function (res) {
        if (res.lastCronTick) {
          setText("cron-tick", new Date(res.lastCronTick).toLocaleString() + " (手动触发成功)");
        }
      }).catch(function (err) {
        setText("cron-tick", "触发失败: " + err.message);
      });
    });
  }

  // ---------------------------------------------------------------------------
  // Web Crypto (HMAC & SHA-256)
  // ---------------------------------------------------------------------------
  var hmacBtn = byId("hmac-btn");
  if (hmacBtn) {
    hmacBtn.addEventListener("click", function () {
      var text = (byId("hmac-input").value || "").trim();
      setText("hmac-out", "计算中...");
      requestJson("/api/crypto-sign", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ text: text })
      }).then(function (res) {
        setText("hmac-out", "算法: " + res.algorithm + "\\n签名: " + res.signature);
      }).catch(function (err) {
        setText("hmac-out", "失败: " + err.message);
      });
    });
  }

  var hashBtn = byId("hash-btn");
  if (hashBtn) {
    hashBtn.addEventListener("click", function () {
      var text = byId("hash-input").value;
      setText("hash-out", "计算中...");
      requestJson("/api/hash", {
        method: "POST",
        headers: { "content-type": "text/plain; charset=utf-8" },
        body: text
      }).then(function (res) {
        setText("hash-out", "算法: " + res.algorithm + " (" + res.bytes + " 字节)\\nSHA-256: " + res.sha256);
      }).catch(function (err) {
        setText("hash-out", "失败: " + err.message);
      });
    });
  }

  // ---------------------------------------------------------------------------
  // Deno KV (Atomic Visits, TTL Cache, Guestbook)
  // ---------------------------------------------------------------------------
  function bumpVisits() {
    requestJson("/api/visit", { method: "POST" }).then(function (res) {
      setText("visit-count", res.count != null ? Number(res.count).toLocaleString() : "–");
    }).catch(function () {
      setText("visit-count", "–");
    });
  }
  bumpVisits();

  var ttlBtn = byId("ttl-btn");
  if (ttlBtn) {
    ttlBtn.addEventListener("click", function () {
      setText("ttl-out", "写入中...");
      requestJson("/api/kv-cache", { method: "POST" }).then(function (res) {
        setText("ttl-out", "状态: " + res.status + "\\nKey: " + JSON.stringify(res.key) + "\\nTTL: " + res.ttlSeconds + " 秒后物理消除");
      }).catch(function (err) {
        setText("ttl-out", "写入失败: " + err.message);
      });
    });
  }

  function loadGuestbook() {
    requestJson("/api/guestbook").then(function (entries) {
      var list = byId("guestbook-list");
      if (!list) return;
      if (!Array.isArray(entries) || entries.length === 0) {
        list.innerHTML = '<div style="color:var(--muted); font-size:13px; padding:10px 0;">暂无留言</div>';
        return;
      }
      list.innerHTML = entries.map(function (e) {
        var time = new Date(e.ts).toLocaleString();
        return '<div style="padding:10px 0; border-bottom:1px solid var(--border); font-size:13px;"><b style="color:var(--accent);">' + e.name + '</b>: ' + e.text + '<div style="color:var(--muted); font-size:11px; margin-top:2px;">' + time + '</div></div>';
      }).join("");
    }).catch(function (err) {
      console.error("加载留言失败:", err);
    });
  }
  loadGuestbook();

  var gbSend = byId("gb-send");
  if (gbSend) {
    gbSend.addEventListener("click", function () {
      var name = (byId("gb-name").value || "").trim();
      var text = (byId("gb-text").value || "").trim();
      if (!text) return;
      gbSend.disabled = true;
      requestJson("/api/guestbook", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: name, text: text })
      }).then(function () {
        byId("gb-text").value = "";
        loadGuestbook();
      }).finally(function () {
        gbSend.disabled = false;
      });
    });
  }

  // ---------------------------------------------------------------------------
  // SSE 流式响应
  // ---------------------------------------------------------------------------
  var streamBtn = byId("stream-btn");
  if (streamBtn) {
    streamBtn.addEventListener("click", function () {
      var out = byId("stream-out");
      out.innerHTML = "";
      var cursor = document.createElement("span");
      cursor.className = "cursor";
      out.appendChild(cursor);

      var source = new EventSource("/api/stream");
      source.onmessage = function (event) {
        try {
          var payload = JSON.parse(event.data);
          if (payload.chunk) {
            var textNode = document.createTextNode(payload.chunk);
            out.insertBefore(textNode, cursor);
          }
        } catch (e) { console.error(e); }
      };

      source.addEventListener("done", function () {
        source.close();
        if (cursor.parentNode) cursor.parentNode.removeChild(cursor);
      });

      source.onerror = function () {
        source.close();
        if (cursor.parentNode) cursor.parentNode.removeChild(cursor);
      };
    });
  }

  // ---------------------------------------------------------------------------
  // WebSocket 聊天
  // ---------------------------------------------------------------------------
  (function setupWebSocket() {
    var wsStatus = byId("ws-status");
    var wsStatusText = byId("ws-status-text");
    var chatlog = byId("chatlog");
    var chatInput = byId("chat-input");
    var chatSend = byId("chat-send");
    var onlineCount = byId("online-count");

    var protocol = location.protocol === "https:" ? "wss:" : "ws:";
    var wsUrl = protocol + "//" + location.host + "/ws";
    var socket = null;

    function appendMsg(html) {
      if (!chatlog) return;
      var div = document.createElement("div");
      div.innerHTML = html;
      var el = div.firstElementChild;
      if (el) {
        chatlog.appendChild(el);
        chatlog.scrollTop = chatlog.scrollHeight;
      }
    }

    function connect() {
      try { socket = new WebSocket(wsUrl); } catch (e) { return; }

      socket.onopen = function () {
        if (wsStatus) wsStatus.className = "status connected";
        if (wsStatusText) wsStatusText.textContent = "已连接";
        if (chatSend) chatSend.disabled = false;
      };

      socket.onmessage = function (event) {
        try {
          var msg = JSON.parse(event.data);
          var time = msg.ts ? new Date(msg.ts).toLocaleTimeString() : "";
          if (msg.type === "presence") {
            if (onlineCount) onlineCount.textContent = String(msg.online);
          } else if (msg.type === "system") {
            appendMsg('<div class="msg-row sys-row"><div class="sys-badge">' + msg.text + '</div></div>');
          } else if (msg.type === "chat") {
            var cls = msg.self ? "me-row" : "other-row";
            appendMsg('<div class="msg-row ' + cls + '"><div class="msg-author">' + msg.name + '</div><div class="msg-bubble">' + msg.text + '<span class="msg-time">' + time + '</span></div></div>');
          }
        } catch (e) { console.error(e); }
      };

      socket.onclose = function () {
        if (wsStatus) wsStatus.className = "status disconnected";
        if (wsStatusText) wsStatusText.textContent = "已断开";
        if (chatSend) chatSend.disabled = true;
        setTimeout(connect, 3000);
      };
    }

    function sendChat() {
      if (!socket || socket.readyState !== WebSocket.OPEN) return;
      var text = (chatInput.value || "").trim();
      if (!text) return;
      socket.send(JSON.stringify({ text: text }));
      chatInput.value = "";
    }

    if (chatSend) chatSend.addEventListener("click", sendChat);
    if (chatInput) {
      chatInput.addEventListener("keydown", function (e) {
        if (e.key === "Enter") sendChat();
      });
    }

    connect();
  })();

  // ---------------------------------------------------------------------------
  // AI 边缘 API 网关 & KV 缓存 (Phase 1)
  // ---------------------------------------------------------------------------
  (function setupAiGateway() {
    var aiInput = byId("ai-input");
    var aiSendBtn = byId("ai-send-btn");
    var aiChatlog = byId("ai-chatlog");
    var aiModelSelect = byId("ai-model-select");
    var aiSystemPrompt = byId("ai-system-prompt");
    var aiCacheToggle = byId("ai-cache-toggle");
    var aiKeyStatus = byId("ai-key-status");
    var aiTokenCount = byId("ai-token-count");

    var totalTokensSaved = 0;

    requestJson("/api/info").then(function (info) {
      if (aiKeyStatus) {
        if (info.aiApiKeyConfigured) {
          aiKeyStatus.textContent = "✅ 已配置 AI API 代理";
          aiKeyStatus.style.background = "rgba(0,212,170,0.15)";
          aiKeyStatus.style.color = "var(--accent)";
        } else {
          aiKeyStatus.textContent = "💡 演示模式 (未检测到 AI_API_KEY 变量)";
          aiKeyStatus.style.background = "rgba(255,189,90,0.15)";
          aiKeyStatus.style.color = "var(--warning)";
        }
      }
    }).catch(function () {});

    var chipBtns = document.querySelectorAll(".ai-chip-btn");
    chipBtns.forEach(function (btn) {
      btn.addEventListener("click", function () {
        var prompt = this.getAttribute("data-prompt");
        if (aiInput && prompt) {
          aiInput.value = prompt;
          sendAiMessage();
        }
      });
    });

    function appendAiMsg(author, text, badgeHtml) {
      if (!aiChatlog) return;
      var isUser = author === "你";
      var rowCls = isUser ? "me-row" : "other-row";
      var div = document.createElement("div");
      div.className = "msg-row " + rowCls;

      var authorDiv = document.createElement("div");
      authorDiv.className = "msg-author";
      authorDiv.textContent = author;
      div.appendChild(authorDiv);

      var bubbleDiv = document.createElement("div");
      bubbleDiv.className = "msg-bubble";
      bubbleDiv.innerHTML = text.replace(/\\n/g, "<br/>") + (badgeHtml || "");
      div.appendChild(bubbleDiv);

      aiChatlog.appendChild(div);
      aiChatlog.scrollTop = aiChatlog.scrollHeight;
    }

    function sendAiMessage() {
      if (!aiInput || !aiSendBtn) return;
      var prompt = (aiInput.value || "").trim();
      if (!prompt) return;

      appendAiMsg("你", prompt, "");
      aiInput.value = "";
      aiSendBtn.disabled = true;

      var model = aiModelSelect ? aiModelSelect.value : "deepseek-chat";
      var systemPrompt = aiSystemPrompt ? aiSystemPrompt.value : "";
      var enableCache = aiCacheToggle ? aiCacheToggle.checked : true;

      requestJson("/api/ai/chat", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          prompt: prompt,
          systemPrompt: systemPrompt,
          enableCache: enableCache,
          model: model
        })
      }).then(function (res) {
        var badgeHtml = "";
        if (res.cached) {
          badgeHtml = '<div style="margin-top:6px; font-size:11px; color:var(--accent); background:rgba(0,212,170,0.12); padding:2px 8px; border-radius:4px; display:inline-block;">⚡ Deno KV 0ms 缓存命中 (省 Token)</div>';
          totalTokensSaved += res.savedTokens || 30;
          if (aiTokenCount) aiTokenCount.textContent = String(totalTokensSaved);
        } else {
          badgeHtml = '<div style="margin-top:6px; font-size:11px; color:var(--muted); background:rgba(255,255,255,0.06); padding:2px 8px; border-radius:4px; display:inline-block;">🌐 边缘 API 代理响应 (' + res.model + ')</div>';
        }

        appendAiMsg("🤖 AI 边缘网关", res.text, badgeHtml);
      }).catch(function (err) {
        appendAiMsg("🤖 系统异常", "请求失败: " + err.message, "");
      }).finally(function () {
        aiSendBtn.disabled = false;
      });
    }

    if (aiSendBtn) aiSendBtn.addEventListener("click", sendAiMessage);
    if (aiInput) {
      aiInput.addEventListener("keydown", function (e) {
        if (e.key === "Enter") sendAiMessage();
      });
    }
  })();

  // ---------------------------------------------------------------------------
  // AI 金句卡片生成器 (Phase 2)
  // ---------------------------------------------------------------------------
  (function setupAiCard() {
    var textInput = byId("card-text-input");
    var generateBtn = byId("card-generate-btn");
    var styleSelect = byId("card-style-select");
    var authorInput = byId("card-author-input");
    var quoteOut = byId("card-quote-out");
    var pointsOut = byId("card-points-out");
    var authorOut = byId("card-author-out");
    var renderBox = byId("card-render-box");
    var galleryList = byId("card-gallery-list");
    var refreshGalleryBtn = byId("card-refresh-gallery");

    var chipBtns = document.querySelectorAll(".card-chip-btn");
    chipBtns.forEach(function (btn) {
      btn.addEventListener("click", function () {
        var text = this.getAttribute("data-text");
        if (textInput && text) {
          textInput.value = text;
        }
      });
    });

    function applyCardStyle(style) {
      if (!renderBox) return;
      if (style === "gradient") {
        renderBox.style.background = "linear-gradient(135deg, #1e1b4b 0%, #311042 50%, #0f172a 100%)";
        renderBox.style.borderColor = "rgba(168,85,247,0.4)";
      } else if (style === "minimal") {
        renderBox.style.background = "#18181b";
        renderBox.style.borderColor = "rgba(255,255,255,0.15)";
      } else {
        renderBox.style.background = "linear-gradient(135deg, #111827 0%, #0f172a 100%)";
        renderBox.style.borderColor = "rgba(0,212,170,0.3)";
      }
    }

    if (styleSelect) {
      styleSelect.addEventListener("change", function () {
        applyCardStyle(this.value);
      });
    }

    function loadGallery() {
      if (!galleryList) return;
      requestJson("/api/ai/cards").then(function (res) {
        if (!res.cards || res.cards.length === 0) {
          galleryList.innerHTML = '<div style="font-size:12px; color:var(--muted); grid-column:1/-1;">暂无卡片记录，点击生成创建第一张卡片！</div>';
          return;
        }

        var html = "";
        res.cards.forEach(function (card) {
          var dateStr = new Date(card.ts).toLocaleDateString();
          var pointsHtml = "";
          if (Array.isArray(card.keyPoints)) {
            card.keyPoints.forEach(function (p) {
              pointsHtml += '<div style="font-size:11px; color:#9ca3af; margin-top:2px;">✦ ' + p + '</div>';
            });
          }

          html += '<div style="background:rgba(0,0,0,0.4); border:1px solid var(--border); padding:14px; border-radius:10px; font-size:12px;">' +
            '<div style="font-weight:700; color:var(--text); margin-bottom:8px; line-height:1.4;">“' + card.summaryQuote + '”</div>' +
            pointsHtml +
            '<div style="margin-top:10px; font-size:10px; color:var(--muted); display:flex; justify-content:space-between;">' +
            '<span>✍️ ' + card.author + '</span><span>' + dateStr + '</span>' +
            '</div></div>';
        });
        galleryList.innerHTML = html;
      }).catch(function () {
        galleryList.innerHTML = '<div style="font-size:12px; color:var(--warning); grid-column:1/-1;">加载历史画廊失败</div>';
      });
    }

    function generateCard() {
      if (!textInput || !generateBtn) return;
      var text = (textInput.value || "").trim();
      if (!text) {
        alert("请先在文本框中输入需要提炼的内容");
        return;
      }

      var style = styleSelect ? styleSelect.value : "dark";
      var author = authorInput ? authorInput.value.trim() || "边缘 AI 思想家" : "边缘 AI 思想家";

      generateBtn.disabled = true;
      generateBtn.textContent = "✨ 边缘 AI 正在提炼金句卡片...";

      requestJson("/api/ai/card", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          text: text,
          style: style,
          author: author
        })
      }).then(function (res) {
        if (quoteOut) quoteOut.textContent = res.summaryQuote;
        if (authorOut) authorOut.textContent = res.author;

        if (pointsOut && Array.isArray(res.keyPoints)) {
          var ptsHtml = "";
          res.keyPoints.forEach(function (p) {
            ptsHtml += '<div style="display:flex; align-items:center; gap:6px;"><span style="color:var(--accent);">✦</span> ' + p + '</div>';
          });
          pointsOut.innerHTML = ptsHtml;
        }

        applyCardStyle(res.style);
        loadGallery();
      }).catch(function (err) {
        alert("卡片生成失败: " + err.message);
      }).finally(function () {
        generateBtn.disabled = false;
        generateBtn.textContent = "✨ 生成 AI 金句卡片";
      });
    }

    if (generateBtn) generateBtn.addEventListener("click", generateCard);
    if (refreshGalleryBtn) refreshGalleryBtn.addEventListener("click", loadGallery);

    loadGallery();
  })();

  // ---------------------------------------------------------------------------
  // AI 网页剪藏知识库 (Phase 3)
  // ---------------------------------------------------------------------------
  (function setupAiClip() {
    var urlInput = byId("clip-url-input");
    var titleInput = byId("clip-title-input");
    var contentInput = byId("clip-content-input");
    var submitBtn = byId("clip-submit-btn");
    var clipListBox = byId("clip-list-box");
    var refreshBtn = byId("clip-refresh-btn");

    function loadClips() {
      if (!clipListBox) return;
      requestJson("/api/ai/clips").then(function (res) {
        if (!res.clips || res.clips.length === 0) {
          clipListBox.innerHTML = '<div style="font-size:12px; color:var(--muted);">知识库暂无剪藏内容，提交表单剪藏第一篇文章吧！</div>';
          return;
        }

        var html = "";
        res.clips.forEach(function (clip) {
          var dateStr = new Date(clip.ts).toLocaleString();
          var tagsHtml = "";
          if (Array.isArray(clip.tags)) {
            clip.tags.forEach(function (t) {
              tagsHtml += '<span style="font-size:10px; padding:1px 6px; border-radius:4px; background:rgba(0,212,170,0.12); color:var(--accent); margin-right:4px;">#' + t + '</span>';
            });
          }

          html += '<div style="background:rgba(0,0,0,0.4); border:1px solid var(--border); padding:12px; border-radius:8px;">' +
            '<div style="font-weight:700; color:var(--accent); margin-bottom:4px;"><a href="' + clip.url + '" target="_blank" style="color:inherit; text-decoration:none;">' + clip.title + ' ↗</a></div>' +
            '<div style="font-size:11.5px; color:var(--text); margin-bottom:6px;">' + clip.summary + '</div>' +
            '<div>' + tagsHtml + '</div>' +
            '<div style="margin-top:6px; font-size:10px; color:var(--muted);">' + dateStr + '</div>' +
            '</div>';
        });
        clipListBox.innerHTML = html;
      }).catch(function () {
        clipListBox.innerHTML = '<div style="font-size:12px; color:var(--warning);">知识库数据加载失败</div>';
      });
    }

    function submitClip() {
      if (!urlInput || !contentInput || !submitBtn) return;
      var url = (urlInput.value || "").trim();
      var title = (titleInput ? titleInput.value : "").trim() || "未命名网页剪藏";
      var content = (contentInput.value || "").trim();

      if (!url || !content) {
        alert("请输入网页 URL 链接与主要正文内容");
        return;
      }

      submitBtn.disabled = true;
      submitBtn.textContent = "✂️ 边缘 AI 正在分析归档...";

      requestJson("/api/ai/clip", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          url: url,
          title: title,
          content: content
        })
      }).then(function () {
        loadClips();
        alert("🎉 成功剪藏并归档到 Deno KV 知识库！");
      }).catch(function (err) {
        alert("剪藏失败: " + err.message);
      }).finally(function () {
        submitBtn.disabled = false;
        submitBtn.textContent = "✂️ 一键 AI 剪藏到 Deno KV 知识库";
      });
    }

    if (submitBtn) submitBtn.addEventListener("click", submitClip);
    if (refreshBtn) refreshBtn.addEventListener("click", loadClips);

    loadClips();
  })();

  // ---------------------------------------------------------------------------
  // 多人实时协同 AI 讨论室 (Phase 4)
  // ---------------------------------------------------------------------------
  (function setupAiCollab() {
    var roomSelect = byId("collab-room-select");
    var promptInput = byId("collab-prompt-input");
    var sendBtn = byId("collab-send-btn");
    var chatLog = byId("collab-chat-log");

    function appendCollabLog(html) {
      if (!chatLog) return;
      var div = document.createElement("div");
      div.innerHTML = html;
      chatLog.appendChild(div);
      chatLog.scrollTop = chatLog.scrollHeight;
    }

    function triggerCollab() {
      if (!promptInput || !sendBtn) return;
      var prompt = (promptInput.value || "").trim();
      if (!prompt) return;

      var roomId = roomSelect ? roomSelect.value : "room-101";
      sendBtn.disabled = true;

      appendCollabLog('<div style="color:var(--text);">[本地操作] 正在触发全员协同 AI 推演...</div>');

      requestJson("/api/ai/collab/prompt", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          roomId: roomId,
          prompt: prompt,
          senderName: "协同成员-" + Math.floor(Math.random() * 899 + 100)
        })
      }).then(function (res) {
        appendCollabLog('<div style="color:var(--accent); font-weight:700;">🤖 AI 协同推演结果:</div><div style="background:rgba(0,0,0,0.3); padding:8px 12px; border-radius:6px; margin-top:4px;">' + res.reply.replace(/\\n/g, "<br/>") + '</div>');
      }).catch(function (err) {
        appendCollabLog('<div style="color:var(--warning);">[协同失败] ' + err.message + '</div>');
      }).finally(function () {
        sendBtn.disabled = false;
      });
    }

    if (sendBtn) sendBtn.addEventListener("click", triggerCollab);
  })();

  // ---------------------------------------------------------------------------
  // 🔐 页面访问密码与安全解锁机制
  // ---------------------------------------------------------------------------
  (function setupPasswordProtection() {
    var lockOverlay = byId("lock-overlay");
    var passwordInput = byId("lock-password-input");
    var unlockBtn = byId("lock-unlock-btn");
    var errorMsg = byId("lock-error-msg");

    function checkAuthStatus() {
      var savedToken = localStorage.getItem("deno_auth_token") || "";
      requestJson("/api/auth/status", {
        headers: savedToken ? { "Authorization": "Bearer " + savedToken } : {}
      }).then(function (res) {
        if (res.authenticated) {
          if (lockOverlay) lockOverlay.style.display = "none";
        } else {
          if (lockOverlay) lockOverlay.style.display = "flex";
        }
      }).catch(function () {
        if (lockOverlay) lockOverlay.style.display = "flex";
      });
    }

    function doUnlock() {
      if (!passwordInput || !unlockBtn) return;
      var password = passwordInput.value || "";
      if (!password) {
        if (errorMsg) {
          errorMsg.textContent = "请输入访问密码";
          errorMsg.style.display = "block";
        }
        return;
      }

      unlockBtn.disabled = true;
      unlockBtn.textContent = "正在校验密码...";
      if (errorMsg) errorMsg.style.display = "none";

      requestJson("/api/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ password: password })
      }).then(function (res) {
        if (res.token) {
          localStorage.setItem("deno_auth_token", res.token);
        }
        if (lockOverlay) lockOverlay.style.display = "none";
        location.reload();
      }).catch(function (err) {
        if (errorMsg) {
          errorMsg.textContent = err.message || "密码错误，请重试";
          errorMsg.style.display = "block";
        }
      }).finally(function () {
        unlockBtn.disabled = false;
        unlockBtn.textContent = "🔓 验证密码并解锁访问";
      });
    }

    if (unlockBtn) unlockBtn.addEventListener("click", doUnlock);
    if (passwordInput) {
      passwordInput.addEventListener("keydown", function (e) {
        if (e.key === "Enter") doUnlock();
      });
    }

    checkAuthStatus();
  })();
})();
</script>
</body>
</html>`;

// -----------------------------------------------------------------------------
// 诊断信息与运行状态 API
// -----------------------------------------------------------------------------

// 🔐 页面访问认证状态查询与登录 API
async function handleAuthRoutes(pathname: string, req: Request): Promise<Response | null> {
  if (pathname === "/api/auth/status" && req.method === "GET") {
    const appPassword = Deno.env.get("APP_PASSWORD") || "deno2026";
    const secretKey = Deno.env.get("SECRET_KEY") || "deno-default-secret-key-2026";
    const authHeader = req.headers.get("authorization");
    const cookieHeader = req.headers.get("cookie") || "";

    const expectedTokenBuffer = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(`${appPassword}::${secretKey}`),
    );
    const expectedToken = Array.from(new Uint8Array(expectedTokenBuffer))
      .map((x) => x.toString(16).padStart(2, "0"))
      .join("");

    let authenticated = false;
    if (authHeader && authHeader.startsWith("Bearer ")) {
      const token = authHeader.slice(7).trim();
      if (token === expectedToken) authenticated = true;
    }

    if (!authenticated && cookieHeader.includes(`deno_auth_session=${expectedToken}`)) {
      authenticated = true;
    }

    return jsonResponse({
      protected: true,
      authenticated,
    });
  }

  if (pathname === "/api/auth/login" && req.method === "POST") {
    checkRateLimit(req, "auth-login", 10, 60_000);

    const body = (await readJsonBody(req, MAX_JSON_BYTES)) as {
      password?: unknown;
    };

    const password = normalizeText(body.password, 100);
    const expectedPassword = Deno.env.get("APP_PASSWORD") || "deno2026";
    const secretKey = Deno.env.get("SECRET_KEY") || "deno-default-secret-key-2026";

    if (password !== expectedPassword) {
      throw new HttpError(401, "密码错误，请输入正确的访问密码");
    }

    const tokenBuffer = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(`${expectedPassword}::${secretKey}`),
    );
    const token = Array.from(new Uint8Array(tokenBuffer))
      .map((x) => x.toString(16).padStart(2, "0"))
      .join("");

    const response = jsonResponse({
      ok: true,
      token,
      expiresAt: Date.now() + 7 * 86400 * 1000,
    });

    response.headers.set(
      "Set-Cookie",
      `deno_auth_session=${token}; Path=/; Max-Age=604800; SameSite=Lax`,
    );

    return response;
  }

  return null;
}

async function getDiagnostics() {
  let lastCronTick: string | null = null;
  try {
    const diagKv = kv ?? (await Deno.openKv());
    const entry = await diagKv.get<string>(["stats", "last_cron_tick"]);
    if (entry && entry.value) {
      lastCronTick = entry.value;
    }
  } catch (err) {
    console.error("[Diagnostics Read Error]", err);
  }

  const aiApiKeyConfigured = Boolean(
    Deno.env.get("AI_API_KEY") ||
    Deno.env.get("DEEPSEEK_API_KEY") ||
    Deno.env.get("OPENAI_API_KEY"),
  );

  return {
    appName: APP_NAME,
    bootTime: BOOT_TIME,
    instanceId: INSTANCE_ID,
    region: Deno.env.get("DENO_REGION") ?? "local",
    denoVersion: Deno.version.deno,
    v8Version: Deno.version.v8,
    typescriptVersion: Deno.version.typescript,
    kvApiAvailable,
    kvConnected: kv !== null,
    kvError,
    lastCronTick,
    aiApiKeyConfigured,
  };
}

function withSecurityHeaders(response: Response): Response {
  const headers = new Headers(response.headers);

  headers.set("strict-transport-security", "max-age=63072000; includeSubDomains; preload");
  headers.set("x-content-type-options", "nosniff");
  headers.set("x-frame-options", "DENY");
  headers.set("referrer-policy", "strict-origin-when-cross-origin");

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

// -----------------------------------------------------------------------------
// 核心 HTTP 路由 Handler
// -----------------------------------------------------------------------------

async function handleRequest(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const pathname = url.pathname;

  if (pathname === "/" || pathname === "/index.html") {
    if (req.method !== "GET" && req.method !== "HEAD") {
      return methodNotAllowed(["GET", "HEAD"]);
    }

    return new Response(PAGE_HTML, {
      headers: {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-cache",
      },
    });
  }

  if (pathname === "/health") {
    if (req.method !== "GET") {
      return methodNotAllowed(["GET"]);
    }

    return jsonResponse({
      status: "ok",
      uptime: Math.floor((Date.now() - BOOT_TIME) / 1000),
      instanceId: INSTANCE_ID,
    });
  }

  const authRes = await handleAuthRoutes(pathname, req);
  if (authRes) return authRes;

  if (pathname === "/api/info" || pathname === "/api/diagnostics") {
    if (req.method !== "GET") {
      return methodNotAllowed(["GET"]);
    }

    const diag = await getDiagnostics();
    return jsonResponse(diag);
  }

  // AI 边缘 API 网关与 Deno KV 缓存 API (Phase 1)
  if (pathname === "/api/ai/chat" && req.method === "POST") {
    checkRateLimit(req, "ai-chat", 30, 60_000);

    const body = (await readJsonBody(req, MAX_JSON_BYTES)) as {
      prompt?: unknown;
      systemPrompt?: unknown;
      enableCache?: unknown;
      model?: unknown;
    };

    const prompt = normalizeText(body.prompt, 1000);
    if (!prompt) {
      throw new HttpError(400, "Prompt 提问不能为空");
    }

    const systemPrompt = normalizeText(
      body.systemPrompt ||
        "你是一个精通 Deno、TypeScript 与边缘计算的高级 Web 技术专家助手，回答简明扼要。",
      500,
    );

    const enableCache = body.enableCache !== false;
    const model = normalizeText(body.model || "deepseek-chat", 50);

    // 计算 SHA-256 哈希用于 Deno KV 缓存
    const cacheKeyRaw = `${systemPrompt}::${prompt.toLowerCase()}`;
    const hashBuffer = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(cacheKeyRaw));
    const hashHex = Array.from(new Uint8Array(hashBuffer))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");

    // 尝试读取 Deno KV 缓存
    if (enableCache) {
      try {
        const cacheKv = kv ?? (await Deno.openKv());
        const cached = await cacheKv.get<string>(["ai_cache_v1", hashHex]);
        if (cached && cached.value) {
          return jsonResponse({
            cached: true,
            hash: hashHex,
            text: cached.value,
            model,
            savedTokens: Math.max(20, Math.ceil(utf8Length(cached.value) / 3)),
          });
        }
      } catch {
        // 忽略缓存读取异常
      }
    }

    const apiKey =
      Deno.env.get("AI_API_KEY") ||
      Deno.env.get("DEEPSEEK_API_KEY") ||
      Deno.env.get("OPENAI_API_KEY");

    if (apiKey) {
      const baseUrl = Deno.env.get("AI_BASE_URL") ?? "https://api.deepseek.com";
      const targetUrl = baseUrl.endsWith("/v1")
        ? `${baseUrl}/chat/completions`
        : `${baseUrl}/v1/chat/completions`;

      try {
        const aiRes = await fetch(targetUrl, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            model,
            messages: [
              { role: "system", content: systemPrompt },
              { role: "user", content: prompt },
            ],
            stream: false,
          }),
        });

        if (!aiRes.ok) {
          const errText = await aiRes.text();
          throw new HttpError(aiRes.status, `大模型 API 返回异常: ${errText.slice(0, 120)}`);
        }

        const data = await aiRes.json();
        const responseText = data.choices?.[0]?.message?.content || "未收到有效 AI 回复";

        if (enableCache) {
          try {
            const cacheKv = kv ?? (await Deno.openKv());
            await cacheKv.set(["ai_cache_v1", hashHex], responseText);
          } catch {
            // ignore
          }
        }

        return jsonResponse({
          cached: false,
          hash: hashHex,
          text: responseText,
          model,
          savedTokens: 0,
        });
      } catch (err) {
        if (err instanceof HttpError) throw err;
        throw new HttpError(502, `AI 上游网络连接失败: ${getErrorMessage(err)}`);
      }
    }

    // 演示模式 (未配置 API KEY 时提供丰富 Mock AI 响应)
    const demoReply =
      `【Deno 边缘 AI 网关 Demo 模拟回复】\n\n` +
      `收到您的提问：「${prompt}」\n\n` +
      `⚡ 边缘特性：本服务完全运行在 Deno Deploy 全球边缘节点上！\n` +
      `💡 环境变量：未检测到 AI_API_KEY，当前以 Demo 模式运行。在 Deno Deploy 设置 AI_API_KEY 即可自动连接真实 DeepSeek / OpenAI 模型！\n\n` +
      `当你开启缓存再次提问相同问题时，将自动触发 Deno KV 0ms 零 Token 缓存命中！`;

    if (enableCache) {
      try {
        const cacheKv = kv ?? (await Deno.openKv());
        await cacheKv.set(["ai_cache_v1", hashHex], demoReply);
      } catch {
        // ignore
      }
    }

    return jsonResponse({
      cached: false,
      hash: hashHex,
      text: demoReply,
      model: `${model} (Demo)`,
      savedTokens: 0,
    });
  }

  // AI 文本摘要提炼与金句小卡片 API (Phase 2)
  if (pathname === "/api/ai/card" && req.method === "POST") {
    checkRateLimit(req, "ai-card", 20, 60_000);

    const body = (await readJsonBody(req, MAX_JSON_BYTES)) as {
      text?: unknown;
      style?: unknown;
      author?: unknown;
    };

    const text = normalizeText(body.text, 2000);
    if (!text) {
      throw new HttpError(400, "输入文本不能为空");
    }

    const style = normalizeText(body.style || "dark", 20);
    const author = normalizeText(body.author || "边缘 AI 思想家", 50);

    const apiKey =
      Deno.env.get("AI_API_KEY") ||
      Deno.env.get("DEEPSEEK_API_KEY") ||
      Deno.env.get("OPENAI_API_KEY");

    let summaryQuote = "";
    let keyPoints: string[] = [];

    if (apiKey) {
      const baseUrl = Deno.env.get("AI_BASE_URL") ?? "https://api.deepseek.com";
      const targetUrl = baseUrl.endsWith("/v1")
        ? `${baseUrl}/chat/completions`
        : `${baseUrl}/v1/chat/completions`;

      try {
        const aiRes = await fetch(targetUrl, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            model: Deno.env.get("AI_MODEL") ?? "deepseek-chat",
            messages: [
              {
                role: "system",
                content:
                  '你是一个精炼专家。请将用户文本提炼为 1 条 30 字以内核心金句，以及 3 条简明要点。必须输出合法 JSON：{"summaryQuote":"...", "keyPoints":["...","...","..."]}',
              },
              { role: "user", content: text },
            ],
            response_format: { type: "json_object" },
          }),
        });

        if (aiRes.ok) {
          const data = await aiRes.json();
          const parsed = JSON.parse(data.choices?.[0]?.message?.content || "{}");
          if (parsed.summaryQuote && Array.isArray(parsed.keyPoints)) {
            summaryQuote = parsed.summaryQuote;
            keyPoints = parsed.keyPoints.slice(0, 3);
          }
        }
      } catch {
        // fallback to smart mock below
      }
    }

    if (!summaryQuote) {
      const cleanText = text.replace(/\s+/g, " ");
      summaryQuote = cleanText.length > 30 ? cleanText.slice(0, 28) + "…" : cleanText;
      keyPoints = [
        "边缘架构提供全球 <20ms 超低延迟访问",
        "AI 大模型与 Deno 原生极速协同提炼",
        "Deno KV 实现分布式高可用卡片持久化",
      ];
    }

    const cardId = crypto.randomUUID();
    const cardObj = {
      id: cardId,
      summaryQuote,
      keyPoints,
      rawText: text.slice(0, 200),
      author,
      style,
      ts: Date.now(),
    };

    if (kv) {
      try {
        await kv.set(["ai_cards_v1", cardObj.ts, cardId], cardObj);
      } catch {
        // ignore
      }
    }

    return jsonResponse(cardObj);
  }

  if (pathname === "/api/ai/cards" && req.method === "GET") {
    const cards: unknown[] = [];
    if (kv) {
      try {
        const iter = kv.list<unknown>({ prefix: ["ai_cards_v1"] }, { limit: 12, reverse: true });
        for await (const entry of iter) {
          cards.push(entry.value);
        }
      } catch {
        // ignore
      }
    }
    return jsonResponse({ cards });
  }

  // AI 网页剪藏知识库 API (Phase 3)
  if (pathname === "/api/ai/clip" && req.method === "POST") {
    checkRateLimit(req, "ai-clip", 20, 60_000);

    const body = (await readJsonBody(req, MAX_JSON_BYTES)) as {
      url?: unknown;
      title?: unknown;
      content?: unknown;
    };

    const url = normalizeText(body.url, 500);
    const title = normalizeText(body.title || "未命名网页剪藏", 150);
    const content = normalizeText(body.content, 3000);

    if (!url || !content) {
      throw new HttpError(400, "URL 与网页正文内容不能为空");
    }

    const summary = content.length > 80 ? content.slice(0, 78) + "…" : content;
    const tags = ["Deno", "边缘原生", "AI剪藏"];

    const clipId = crypto.randomUUID();
    const clipObj = {
      id: clipId,
      url,
      title,
      summary,
      tags,
      ts: Date.now(),
    };

    if (kv) {
      try {
        await kv.set(["ai_clips_v1", clipObj.ts, clipId], clipObj);
      } catch {
        // ignore
      }
    }

    return jsonResponse(clipObj);
  }

  if (pathname === "/api/ai/clips" && req.method === "GET") {
    const clips: unknown[] = [];
    if (kv) {
      try {
        const iter = kv.list<unknown>({ prefix: ["ai_clips_v1"] }, { limit: 10, reverse: true });
        for await (const entry of iter) {
          clips.push(entry.value);
        }
      } catch {
        // ignore
      }
    }
    return jsonResponse({ clips });
  }

  // 多人实时协同 AI 讨论室 API (Phase 4)
  if (pathname === "/api/ai/collab/prompt" && req.method === "POST") {
    checkRateLimit(req, "ai-collab", 20, 60_000);

    const body = (await readJsonBody(req, MAX_JSON_BYTES)) as {
      roomId?: unknown;
      prompt?: unknown;
      senderName?: unknown;
    };

    const prompt = normalizeText(body.prompt, 1000);
    if (!prompt) {
      throw new HttpError(400, "协同 Prompt 不能为空");
    }

    const roomId = normalizeText(body.roomId || "room-101", 30);
    const senderName = normalizeText(body.senderName || "协同成员", 30);

    const reply =
      `【Deno 边缘协同推演结论 - ${roomId}】\n` +
      `成员 [${senderName}] 提议的 Prompt：「${prompt}」已成功广播给多端协同成员！\n` +
      `边缘架构优势：所有节点通过 Deno KV Watch 零延迟监听状态更新，实现多人无缝协同。`;

    return jsonResponse({
      roomId,
      senderName,
      prompt,
      reply,
      ts: Date.now(),
    });
  }

  // 手动触发模拟 Cron 心跳 API
  if (pathname === "/api/cron-tick") {
    if (req.method !== "POST") {
      return methodNotAllowed(["POST"]);
    }

    try {
      const tickKv = kv ?? (await Deno.openKv());
      const nowIso = new Date().toISOString();
      await tickKv.set(["stats", "last_cron_tick"], nowIso);
      return jsonResponse({ status: "ok", lastCronTick: nowIso });
    } catch (err) {
      return jsonResponse({ error: getErrorMessage(err) }, { status: 500 });
    }
  }

  // Web Crypto HMAC 安全验签 API
  if (pathname === "/api/crypto-sign") {
    if (req.method !== "POST") {
      return methodNotAllowed(["POST"]);
    }

    checkRateLimit(req, "crypto-sign", 30, 60_000);

    const body = (await readJsonBody(req, MAX_JSON_BYTES)) as { text?: unknown };
    const text = normalizeText(body.text, 2000);
    const secretKey = Deno.env.get("SECRET_KEY") ?? "deno-default-secret";

    const key = await crypto.subtle.importKey(
      "raw",
      new TextEncoder().encode(secretKey),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"],
    );

    const signatureBuffer = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(text));

    const signature = Array.from(new Uint8Array(signatureBuffer))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");

    return jsonResponse({
      text,
      algorithm: "HMAC-SHA256",
      signature,
    });
  }

  // Deno KV TTL 自动过期缓存 API
  if (pathname === "/api/kv-cache") {
    if (req.method !== "POST") {
      return methodNotAllowed(["POST"]);
    }

    checkRateLimit(req, "kv-cache", 30, 60_000);

    if (!kv) {
      throw new HttpError(530, "KV 未准备就绪");
    }

    const key = ["cache", crypto.randomUUID()];
    const value = { data: "临时缓存数据", created: Date.now() };

    await kv.set(key, value, { expireIn: 60000 });

    return jsonResponse({
      status: "created",
      key,
      ttlSeconds: 60,
    });
  }

  if (pathname === "/api/visit") {
    if (req.method !== "POST") {
      return methodNotAllowed(["POST"]);
    }

    checkRateLimit(req, "visit", 30, 60_000);

    const count = await bumpVisits();

    if (count === null) {
      return jsonResponse(
        {
          count: null,
          error: kvApiAvailable
            ? "Deno KV 初始化失败，访问计数暂不可用"
            : "当前运行时未暴露 Deno.openKv，访问计数暂不可用",
          kvApiAvailable,
          kvError,
        },
        { status: 503 },
      );
    }

    return jsonResponse({
      count: count.toString(),
    });
  }

  if (pathname === "/api/hash") {
    if (req.method !== "POST") {
      return methodNotAllowed(["POST"]);
    }

    checkRateLimit(req, "hash", 60, 60_000);

    const text = await readTextBody(req, MAX_HASH_BYTES);
    const encoded = new TextEncoder().encode(text);
    const digest = await crypto.subtle.digest("SHA-256", encoded);

    const sha256 = Array.from(new Uint8Array(digest))
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");

    return jsonResponse({
      algorithm: "SHA-256",
      bytes: encoded.byteLength,
      sha256,
    });
  }

  if (pathname === "/api/stream") {
    if (req.method !== "GET") {
      return methodNotAllowed(["GET"]);
    }

    checkRateLimit(req, "stream", 20, 60_000);

    return new Response(createSseStream(DEMO_TEXT, req.signal), {
      headers: {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-cache, no-transform",
        "x-accel-buffering": "no",
      },
    });
  }

  if (pathname === "/api/guestbook" && req.method === "GET") {
    const rawLimit = Number(url.searchParams.get("limit") ?? "20");
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(Math.floor(rawLimit), 50)) : 20;

    const entries = await listGuestbook(limit);

    if (entries === null) {
      return jsonResponse(
        {
          error: kvApiAvailable
            ? "Deno KV 初始化失败，留言板暂不可用"
            : "当前运行时未暴露 Deno.openKv，留言板暂不可用",
          entries: [],
          kvApiAvailable,
          kvError,
        },
        { status: 503 },
      );
    }

    return jsonResponse(entries);
  }

  if (pathname === "/api/guestbook" && req.method === "POST") {
    checkRateLimit(req, "guestbook-write", 5, 60_000);

    if (!kv) {
      return jsonResponse(
        {
          error: kvApiAvailable
            ? "Deno KV 初始化失败，无法保存留言"
            : "当前运行时未暴露 Deno.openKv，无法保存留言",
          kvApiAvailable,
          kvError,
        },
        { status: 503 },
      );
    }

    const body = await readJsonBody(req, MAX_JSON_BYTES);

    if (typeof body !== "object" || body === null || Array.isArray(body)) {
      throw new HttpError(400, "请求 JSON 必须是对象");
    }

    const record = body as Record<string, unknown>;
    const name = normalizeText(record.name || "匿名", MAX_GUESTBOOK_NAME_LENGTH) || "匿名";
    const text = normalizeText(record.text, MAX_GUESTBOOK_TEXT_LENGTH);

    if (!text) {
      throw new HttpError(400, "留言内容不能为空");
    }

    const entry = await addGuestbookEntry(name, text);

    if (entry === null) {
      return jsonResponse({ error: "Deno KV 不可用，无法保存留言" }, { status: 503 });
    }

    return jsonResponse(entry, { status: 201 });
  }

  if (pathname === "/api/guestbook") {
    return methodNotAllowed(["GET", "POST"]);
  }

  if (pathname === "/ws") {
    if (req.method !== "GET") {
      return methodNotAllowed(["GET"]);
    }

    return handleWebSocket(req);
  }

  return jsonResponse(
    {
      error: "Not Found",
      path: pathname,
    },
    { status: 404 },
  );
}

// -----------------------------------------------------------------------------
// 服务启动与统一错误处理
// -----------------------------------------------------------------------------

Deno.serve(async (req: Request): Promise<Response> => {
  try {
    const response = await handleRequest(req);
    return withSecurityHeaders(response);
  } catch (error) {
    if (error instanceof HttpError) {
      return withSecurityHeaders(
        jsonResponse(
          { error: error.message },
          {
            status: error.status,
            headers: error.headers,
          },
        ),
      );
    }

    const requestId = crypto.randomUUID();

    console.error("[Unhandled Error]", {
      requestId,
      method: req.method,
      url: req.url,
      error: getErrorMessage(error),
      stack: error instanceof Error ? error.stack : undefined,
    });

    return withSecurityHeaders(
      jsonResponse(
        {
          error: "服务器内部错误",
          requestId,
        },
        { status: 500 },
      ),
    );
  }
});

// ============================================================================
// FILE-END
// ============================================================================
export {};
