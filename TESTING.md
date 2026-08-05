# 🧪 Deno Deploy 边缘应用测试用例指南 (Test Cases)

本文档包含了 `deno-case` 项目的**自动化测试用例**与**手动 UI / API 测试用例清单**。

---

## 模块一：后端 API 接口测试矩阵 (API Test Cases)

| 用例 ID       | 测试项 / 接口        | 请求方法 / URL          | 输入参数 / Payload                     | 预期结果                                                                                       |
| :------------ | :------------------- | :---------------------- | :------------------------------------- | :--------------------------------------------------------------------------------------------- |
| **TC-API-01** | 健康检查             | `GET /health`           | 无                                     | 返回 `HTTP 200`，JSON 包含 `status: "ok"`、`uptime` 与 `instanceId`。                          |
| **TC-API-02** | 运行时诊断信息       | `GET /api/info`         | 无                                     | 返回 `HTTP 200`，JSON 包含 `denoVersion`、`v8Version`、`kvConnected` 与 `lastCronTick`。       |
| **TC-API-03** | Web Crypto HMAC 签名 | `POST /api/crypto-sign` | `{"text": "Hello Deno HMAC"}`          | 返回 `HTTP 200`，JSON 包含 64 位 Hex `signature` 与 `algorithm: "HMAC-SHA256"`。               |
| **TC-API-04** | SHA-256 哈希计算     | `POST /api/hash`        | Body 纯文本 `"Hello Deno"`             | 返回 `HTTP 200`，JSON 包含 64 位 Hex `sha256` 摘要。                                           |
| **TC-API-05** | Deno KV TTL 临时缓存 | `POST /api/kv-cache`    | 无                                     | 返回 `HTTP 200`（或 530 未连接时），JSON 包含 `status: "created"`、`key` 与 `ttlSeconds: 60`。 |
| **TC-API-06** | Deno KV 留言板读取   | `GET /api/guestbook`    | Query `?limit=5`                       | 返回 `HTTP 200`，JSON 为留言数组，包含 `id`、`name`、`text`、`ts`。                            |
| **TC-API-07** | Deno KV 留言板提交   | `POST /api/guestbook`   | `{"name":"测试者", "text":"提交测试"}` | 返回 `HTTP 201`，返回创建好的留言条目。                                                        |
| **TC-API-08** | SSE 打字机流输出     | `GET /api/stream`       | Header: `Accept: text/event-stream`    | 返回 `HTTP 200` 响应流，数据格式为 `data: {"chunk":"..."}\n\n`，末尾推送 `event: done`。       |
| **TC-API-09** | WebSocket 升级       | `GET /ws`               | Header: `Upgrade: websocket`           | 返回 `HTTP 101` 升级协议，建立双向 WS 连接，发送/接收 JSON 格式聊天消息。                      |

---

## 模块二：前端 Dashboard 页面交互测试矩阵 (UI Test Cases)

| 用例 ID      | 功能模块                    | 操作步骤                                                             | 预期 UI 交互结果                                                                   |
| :----------- | :-------------------------- | :------------------------------------------------------------------- | :--------------------------------------------------------------------------------- |
| **TC-UI-01** | 侧边栏 (Side Menu) 导航切板 | 点击左侧 Side Menu 菜单项（如 运行时概览、Web Crypto、WebSocket 等） | 右侧主面板平滑切换对应 Tab 视图，所选菜单项高亮高亮显示。                          |
| **TC-UI-02** | 移动端 Drawer 抽屉          | 切换到移动端尺寸，点击顶栏 `☰ 菜单` 按钮                            | 侧边栏从左侧滑出，选择菜单后自动收起。                                             |
| **TC-UI-03** | HMAC 验签计算器             | 在 Web Crypto 面板输入框输入文本，点击 `HMAC 签名` 按钮              | 页面下方结果框实时展示生成的 HMAC-SHA256 64 位 Hex 签名串。                        |
| **TC-UI-04** | SHA-256 计算器              | 在输入框输入任意文本，点击 `SHA-256` 按钮                            | 下方框实时显示计算后的十六进制摘要字符串。                                         |
| **TC-UI-05** | WebSocket 即时聊天室        | 在聊天框输入文字后回车，或开启两个浏览器窗口交互                     | 消息气泡实时展示，右侧为自己（绿色），左侧为他人（灰色），右上方显示实时在线人数。 |
| **TC-UI-06** | SSE Token 打字机动效        | 点击 `▶ 开始 SSE 流式输出` 按钮                                      | 区域内逐字符打印，光标闪烁，打印完毕光标自动消失。                                 |
| **TC-UI-07** | Deno KV TTL 缓存写入        | 点击 `写入 60s TTL 临时缓存` 按钮                                    | 下方显示 Key 路径以及 60 秒倒计时消除提示。                                        |
| **TC-UI-08** | Deno KV 动态留言板          | 填写昵称与内容，点击 `提交留言` 按钮                                 | 按钮变灰，写入成功后清空输入框，下方列表自动加载最新留言。                         |
| **TC-UI-09** | Deno Cron 边缘心跳          | 观察 ⏱️ Deno Cron 面板的心跳时间                                     | 每隔 1 分钟（Cron * * * * * 触发），页面显示的最新心跳时间戳自动刷新。             |

---

## 模块三：自动化测试运行方法

在项目本地终端中运行以下命令：

```bash
# 运行 Deno 原生单元测试
npm test
# 或
deno test --allow-net --allow-env main_test.ts
```
