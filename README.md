# 🦕 Deno Deploy 边缘原生能力全景控制台 & AI 全栈微应用平台

本项目是一个基于 **Deno & Deno Deploy** 的单文件全栈应用。无需繁重前端框架与打包构建，直接以原生的 TypeScript 与 Web 标准 API 驱动，既展示了边缘网络的原生能力，又集成了现代 AI 网关与全栈微应用服务。

---

## 📖 新手快速了解：什么是 Deno 与边缘计算？

- **Deno**：由 Node.js 之父开发的下一代 JavaScript / TypeScript 运行时。原生支持 TypeScript、开箱即用的安全性沙箱，且零配置。
- **Deno Deploy (边缘网络)**：将你的代码同时分发部署到全球 300+ 个 CDN 节点上。用户在东京访问就在东京运行，用户在北京访问就在北京运行，实现 **< 20ms 的超低延迟**。

---

## 🌟 核心功能板块总览

项目划分为了两大核心功能区：

### ⚡ 模块一：边缘基础设施能力展示

1. **⚡ 实时运行时概览 (Runtime & Env)**：实时读取 Deno 运行时版本、V8 引擎版本、部署节点 Region 参数。
2. **🔐 Web Crypto API**：
   - SHA-256 原生摘要哈希计算。
   - **HMAC-SHA256 安全签名 (`/api/crypto-sign`)**：零第三方依赖带秘钥安全防篡改签名。
3. **💬 WebSocket & Deno KV Watch**：结合 WebSockets 与 Deno KV 跨节点广播，实现多人即时气泡聊天室。
4. **🌊 SSE (Server-Sent Events) 流式输出**：基于 `ReadableStream` 的 Token 打字机流式动效。
5. **📈 Deno KV 分布式数据库**：
   - 原子计数器：`kv.atomic().sum()` 原子的提交访问量。
   - **TTL 自动过期缓存 (`/api/kv-cache`)**：支持 60 秒自动物理消除的临时缓存条目。
   - 持久化留言板：`kv.set()` 与 `kv.list()` 时间倒序查询。
6. **⏱️ Deno Cron 边缘定时心跳**：原生 `Deno.cron` 在云端后台按规则 (`* * * * *`) 运行心跳打卡。

### 🤖 模块二：AI 全栈微应用路线图

7. **🤖 AI 边缘 API 网关与 Token 缓存中转站 (Phase 1 - 已完成)**：
   - **兼容大模型通用协议**：支持 DeepSeek、OpenAI、SiliconFlow 等 API 代理。
   - **⚡ Deno KV 智能 Token 缓存**：对 Prompt 进行 SHA-256 哈希作为键，热门提问第二次访问直接从 Deno KV **0ms 零 Token 消耗返回**！
   - **Demo 模式保障**：在未配置真实 `AI_API_KEY` 时自动启动高保真 Mock 响应，确保全功能顺畅体验。
8. **📝 AI 文本摘要提炼与金句排版小卡片生成器 (Phase 2 - 已完成)**：
   - **智能金句提炼**：输入长文本段落，大模型自动精炼 30 字核心金句与 3 条精简观点清单 (`POST /api/ai/card`)。
   - **多风格高颜值卡片**：支持“极客暗黑 (Cyber Dark)”、“渐变霓虹 (Neon Gradient)”与“极简冷灰 (Minimal Gray)”一键排版渲染。
   - **Deno KV 持久化画廊**：生成的卡片自动持久化到 Deno KV 数据库，在 `📚 Deno KV 持久化卡片画廊` (`GET /api/ai/cards`) 中倒序随时回溯查看。
9. **✂️ AI 网页剪藏与个人知识中心 (Phase 3 - 已完成)**：
   - **网页扩展模拟剪藏**：输入 URL 链接与网页正文，AI 自动提炼核心总结并生成标签 (`POST /api/ai/clip`)。
   - **Deno KV 知识库**：知识条目持久化存储于 Deno KV，支持按时间倒序列表回溯 (`GET /api/ai/clips`)。
10. **👥 多人实时协同 AI 讨论室 (Phase 4 - 已完成)**：

- **多端协同编辑 Prompt**：结合 WebSocket + Deno KV Watch，多人在同一房间协同修改 Prompt 与参数。
- **广播全员推演**：一人触发 AI 边缘推演，推演结论与打字机响应全员实时同步广播 (`POST /api/ai/collab/prompt`)。

---

## 🛠️ 新手开发与代码校验指南

在本地开发时，你可以使用标准的 Node / npm 命令来进行严格的代码质量校验。

### 1. 安装项目依赖

```bash
npm install
```

### 2. 三重代码质量校验命令 (推荐在提交前运行)

```bash
# 1. 运行 Prettier 一键自动代码美化
npm run format

# 2. 运行 ESLint 语法规范检查
npm run lint

# 3. 运行 TypeScript 全局类型检查 (tsc --noEmit)
npm run check

# 4. 运行 Deno 单元自动化测试
npm test
```

### 3. 本地启动服务

```bash
# 方式 A：使用 Deno 启动本地开发服务 (推荐，带热更新)
deno task dev

# 方式 B：使用 npm 启动
npm start
```

启动后，在浏览器访问 `http://localhost:8000` 即可查看项目控制台。

---

## ⚙️ 环境变量配置指南 (Environment Variables)

你可以通过项目根目录的 `.env` 文件（本地）或 Deno Deploy 控制台（线上）配置以下变量：

| 变量名         | 说明                                  | 示例值                           |
| :------------- | :------------------------------------ | :------------------------------- |
| `SECRET_KEY`   | HMAC 签名验签的密钥                   | `deno-default-secret-key-2026`   |
| `APP_PASSWORD` | 🔐 页面访问保护密码 (默认 `deno2026`) | `my_custom_password_123`         |
| `AI_API_KEY`   | 大模型 API Key (DeepSeek / OpenAI)    | `sk-xxxxxxxxx`                   |
| `AI_BASE_URL`  | 大模型 API 地址 (可选)                | `https://api.deepseek.com`       |
| `AI_MODEL`     | 调用的模型名称 (可选)                 | `deepseek-chat` 或 `gpt-4o-mini` |

---

## 🚀 部署至 Deno Deploy (上线教程)

### 方式 A：GitHub 关联自动部署 (推荐)

1. 将本地代码推送到 GitHub 仓库。
2. 打开 [Deno Deploy 控制台](https://console.deno.com) 点击 **+ New App** 关联该仓库。
3. Entrypoint 设置为 `main.ts`。
4. 在 **Settings -> Environment Variables** 中配置所需变量。
5. 保存后系统在每次 `git push` 时会自动构建并发布！

### 方式 B：本地命令行一键发布 (deployctl)

设置本地 Access Token 环境变量后，在终端执行：

```bash
npm run deploy -- --project=你的项目名称
```

---

## 🛡️ 自动化 Git Pre-commit 质量守护

项目已配置 Git `.git/hooks/pre-commit` 钩子。每次执行 `git commit` 时，系统将自动依次执行 **Prettier 代码美化检查 -> ESLint 检查 -> TypeScript 类型检查**。只有三项检查 100% 通过后才会完成提交，从源头杜绝带病代码上云。
