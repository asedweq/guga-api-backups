# syntax=docker/dockerfile:1
# ─── Build ────────────────────────────────────────────────────────────────────
FROM node:22-alpine AS builder
WORKDIR /app

# Install dev-deps first so this layer is cached between src-only changes.
# Use package-lock.json for reproducible installs.
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci --ignore-scripts

# Compile TypeScript → CommonJS in dist/
COPY src ./src
RUN npm run build

# ─── Runtime ──────────────────────────────────────────────────────────────────
FROM node:22-alpine
WORKDIR /app

# No runtime npm dependencies — only built JS files are needed.
COPY --from=builder /app/dist ./dist

ENV PORT=3000
EXPOSE 3000

HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -qO- http://localhost:${PORT}/healthz || exit 1

CMD ["node", "dist/index.js"]
