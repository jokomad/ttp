# ─── Build Stage ─────────────────────────────────────────────
# Node 20 Alpine keeps the image small. better-sqlite3 requires
# native compilation, so we need build tools in the build step.
FROM node:20-alpine AS builder

WORKDIR /app

# Install native build tools required by better-sqlite3
RUN apk add --no-cache python3 make g++

COPY package*.json ./
RUN npm ci --omit=dev

# ─── Runtime Stage ────────────────────────────────────────────
FROM node:20-alpine

WORKDIR /app

# Copy only production node_modules + app files
COPY --from=builder /app/node_modules ./node_modules
COPY package.json ./
COPY server.js ./
COPY mongo.js ./
COPY public/ ./public/

# /data is the mount point for the Northflank persistent volume.
# The DB_PATH env var should point here: /data/grid.db
VOLUME ["/data"]

EXPOSE 3000

CMD ["node", "server.js"]
