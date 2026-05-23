# syntax=docker/dockerfile:1
# Build context: ./frontend
#
# Runtime env vars (injected into nginx via envsubst at startup):
#   CORE_HOST  — hostname of the synapse core service  (default: synapse)
#   CORE_PORT  — port of the synapse core service       (default: 80)

# ── Stage 1: Vite build ───────────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

# Cache deps layer separately from source
COPY package.json package-lock.json* ./
RUN npm ci --prefer-offline 2>/dev/null || npm install

COPY . .
RUN npm run build


# ── Stage 2: nginx static file server with API proxy ─────────────────────────
FROM nginx:1.27-alpine AS runtime

RUN rm /etc/nginx/conf.d/default.conf

# nginx template processing: files in /etc/nginx/templates/*.template
# are run through envsubst at startup → written to /etc/nginx/conf.d/
COPY nginx.conf /etc/nginx/templates/default.conf.template

COPY --from=builder /app/dist /usr/share/nginx/html

# Defaults — point at the k8s service name for the synapse core
ENV CORE_HOST=synapse \
    CORE_PORT=80

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
