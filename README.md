# 🦕 Deno Deploy 边缘原生能力全景控制台 (deno-case)

本项目展示了基于 Deno & Deno Deploy 的单文件边缘应用，无需繁重前端框架与 node_modules 依赖即可实现完整边缘原生能力全景 Dashboard。

## 🌟 核心特性与能力展示

1. **⚡ 实时运行时概览 (Runtime & Env)**：从 `Deno.version` 与 `Deno.env` 提取部署节点、区域与运行时环境参数。
2. **🔐 Web Crypto API**：
   - SHA-256 原生哈希计算
   - **HMAC 安全签名 API** (`/api/crypto-sign`)：零第三方依赖带秘钥安全验签
3. **💬 WebSocket & KV Watch**：跨节点/实例实时气泡聊天室与在线人数。
4. **🌊 SSE (Server-Sent Events) 流式输出**：基于 `ReadableStream` 的 Token 打字机实时打字效果。
5. **📈 Deno KV 数据中心**：
   - 分布式原子计数器 (`kv.atomic().sum()`)
   - **KV TTL 自动过期缓存** (`/api/kv-cache`)：支持指定 `expireIn` 自动物理清理
   - 持久化留言板 (`kv.set()` / `kv.list()`)
6. **⏱️ Deno Cron 边缘定时任务**：原生 `Deno.cron` 边缘心跳打卡 (`* * * * *`)。

---

## 🛠️ 本地开发与校验

### 安装依赖 (用于 Node / npm 检查环境)

```bash
npm install
```

### 语法与类型校验 (ESLint & TypeScript)

```bash
# 运行 ESLint 代码检查
npm run lint

# 自动修复 ESLint 问题
npm run lint:fix

# 运行 TypeScript 类型检查
npm run check
```

### 运行服务 (基于 Deno)

```bash
# 启动本地开发服务
deno task dev

# 或使用 npm
npm start
```

---

## 🚀 部署至 Deno Deploy

1. 将代码推送到 GitHub 仓库。
2. 在 [Deno Deploy 控制台](https://dash.deno.com) 新建项目。
3. 关联该 GitHub 仓库，入口文件设置为 `main.ts`。
4. 在 Environment Variables 中配置 `SECRET_KEY`（可选）。
5. 成功发布后即可使用 Deno KV、Deno Cron 与 Web Crypto 边缘原生服务！

---

## 🔒 自动 pre-commit 语法检查

项目集成了 Git `.git/hooks/pre-commit` 钩子。每次执行 `git commit` 时，系统将自动触发 ESLint 代码检查与 TypeScript 类型检查，确保代码语法质量无误。
