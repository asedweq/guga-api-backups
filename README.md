# 客户端续流网关 (CSG — Client Stream Gateway)

CSG 是一个极简的透明反向代理，作用是让 **不做任何修改的 AI 客户端**（OpenAI SDK、Claude SDK、Cherry Studio、curl 等）调用 咕嘎API 节点时，能绕过 Replit Autoscale GFE 的 300s 入站连接墙，让 AI 长流（扩展思考、长上下文生成等）稳定跑完。

```
客户端 ──普通 HTTP/SSE──▶ CSG ⇄ 节点 (/csg/* 端点)
         一条长连接             多条短取流 (每条 <240s)
```

---

## ⚠️ 必须跑在哪里

**CSG 绝对不能部署在 Replit Autoscale / Cloud Run / Vercel Functions 等有硬性入站连接时限的 PaaS 上。** 它面向客户端的那一段本身就是一条长连接，跑 Autoscale 等于撞同一面墙。

推荐宿主：
- 本地 macOS / Linux / Windows 机器
- 家用 NAS（群晖、QNAP 等）
- VPS（Hetzner、DigitalOcean 等）

---

## URL 格式

```
https://<csg-host>:<port>/<node-host>/<原始端点>
```

| 部分 | 含义 |
|---|---|
| `<csg-host>` | CSG 进程的域名或 IP |
| `<node-host>` | 节点的公网域名（如 `foo-node.replit.app`） |
| `/<原始端点>` | 与直连节点时完全一致的路径和查询参数 |

### 示例

```
# 直连节点（受 300s 限制）
https://foo-node.replit.app/v1/chat/completions

# 经过 CSG（绕过 300s 墙）
http://csg.local:3000/foo-node.replit.app/v1/chat/completions
```

客户端只需把 `base_url` 从 `https://foo-node.replit.app` 改为
`http://csg.local:3000/foo-node.replit.app`，其余代码零改动。

---

## 快速启动

### Docker（推荐）

```bash
# 拉取并运行（镜像需自行构建，见下方）
docker run -d \
  --name csg \
  --restart unless-stopped \
  -p 3000:3000 \
  csg:latest
```

### Docker Compose

```yaml
services:
  csg:
    image: csg:latest
    build: .
    ports:
      - "3000:3000"
    environment:
      PORT: "3000"
    restart: unless-stopped
```

### 直接用 Node.js（开发用）

```bash
cd tools/csg
npm install
npm run dev        # tsx 实时编译
# 或
npm run build && npm start
```

---

## 构建 Docker 镜像

### 单平台（当前机器架构）

```bash
cd tools/csg
docker build -t csg:latest .
```

### 多平台 x86-64 + ARM64（发布用）

```bash
docker buildx create --use --name multiarch 2>/dev/null || true
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t yourrepo/csg:latest \
  --push \
  .
```

---

## 配置

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `PORT` | `3000` | CSG 监听端口 |
| `NODE_PROTOCOL` | `https` | 与节点通信的协议。正式部署永远用 `https`（节点是 `*.replit.app`）。仅在本地开发测试时设为 `http`（此时 API Key 会明文传输，只在内网使用） |

CSG 本身**不存储任何密钥**，客户端的 `Authorization` 头原样透传给节点。

### 本地开发测试

```bash
# 将本机运行的节点（http，无 TLS）暴露给 CSG
PORT=3000 NODE_PROTOCOL=http node dist/index.js

# 访问时把节点地址填入第一段路径
curl http://localhost:3000/localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer Re12345679" \
  -d '{"model":"...","messages":[...]}'
```

---

## 客户端配置示例

### OpenAI Python SDK

```python
from openai import OpenAI

client = OpenAI(
    api_key="Re12345679",           # 你的 Proxy Key
    base_url="http://csg.local:3000/foo-node.replit.app/v1",
)

response = client.chat.completions.create(
    model="claude-opus-4-5-thinking",
    messages=[{"role": "user", "content": "Write a 10000-word essay..."}],
    stream=True,
)
for chunk in response:
    print(chunk.choices[0].delta.content or "", end="", flush=True)
```

### OpenAI Node.js SDK

```ts
import OpenAI from "openai";

const client = new OpenAI({
  apiKey: "Re12345679",
  baseURL: "http://csg.local:3000/foo-node.replit.app/v1",
});
```

### Anthropic Python SDK（通过 /v1/messages 透传）

```python
import anthropic

client = anthropic.Anthropic(
    api_key="Re12345679",
    base_url="http://csg.local:3000/foo-node.replit.app",
)
```

### curl

```bash
curl http://csg.local:3000/foo-node.replit.app/v1/chat/completions \
  -H "Authorization: Bearer Re12345679" \
  -H "Content-Type: application/json" \
  -d '{"model":"claude-opus-4-5","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

---

## 特殊端点

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/healthz` | 健康检查，返回 `{"ok":true}`（Docker HEALTHCHECK 用这个） |

---

## 故障排查

| 现象 | 原因 | 解决 |
|---|---|---|
| `502 node_unreachable` | CSG 连不到节点 | 检查节点域名 / 节点是否在线 |
| `401` 透传给客户端 | Proxy Key 错误 | 检查客户端的 `api_key` |
| `502 session_expired` | 节点 90s 内无 poll（正常不会发生，除非 CSG 重启） | 重试请求 |
| 流中途断开 | 节点上游出错 | 查节点日志（`/api/logs`） |
| CSG 自己 300s 挂掉 | CSG 跑在了 Autoscale / 有入站限制的 PaaS | 把 CSG 迁到不限制入站连接时长的宿主 |

---

## 安全说明

- CSG **不验证**客户端的密钥，也不维护域名白名单。URL 里填错节点域名自然连不通。
- 如需限制哪些客户端可以访问 CSG，在 CSG 前面加 Nginx/Caddy 做 IP 白名单或 mTLS 即可。
- CSG 与节点之间的通信走 HTTPS（节点域名是 `*.replit.app`，有正规 TLS）。
