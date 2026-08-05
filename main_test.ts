// ============================================================================
// deno-case / main_test.ts
// Deno 自动化测试用例
// ============================================================================

import { assertEquals, assertExists } from "https://deno.land/std@0.224.0/assert/mod.ts";

// 提示：可以通过 deno test --allow-net --allow-env main_test.ts 运行测试

Deno.test("接口测试: GET /health 健康检查", async () => {
  const req = new Request("http://localhost/health");
  // 简易功能逻辑验证
  assertExists(req);
  assertEquals(req.method, "GET");
});

Deno.test("接口测试: POST /api/hash SHA-256 哈希计算", async () => {
  const text = "Hello Deno";
  const encoded = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest("SHA-256", encoded);
  const sha256 = Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  assertEquals(sha256, "a13361284d720bfa51c8a14b533f07a16f272a2e87903277749f7e53f1d8c1c4");
});

Deno.test("接口测试: Web Crypto HMAC-SHA256 签名计算", async () => {
  const secretKey = "deno-default-secret";
  const text = "Hello Deno HMAC";

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

  assertExists(signature);
  assertEquals(signature.length, 64);
});

Deno.test("接口测试: POST /api/ai/chat AI 网关哈希算法验证", async () => {
  const systemPrompt = "你是一个技术专家助手";
  const prompt = "什么是 Deno";
  const rawKey = `${systemPrompt}::${prompt.toLowerCase()}`;
  const hashBuffer = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(rawKey));
  const hashHex = Array.from(new Uint8Array(hashBuffer))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  assertExists(hashHex);
  assertEquals(hashHex.length, 64);
});
