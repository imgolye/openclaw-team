ARG OPENCLAW_VERSION=2026.3.28

FROM node:22-bookworm-slim AS frontend-build

WORKDIR /app/frontend
COPY apps/frontend/package.json apps/frontend/package-lock.json ./
RUN npm ci
COPY apps/frontend/ ./
RUN npm run build


FROM node:22-bookworm-slim AS llama-server-build

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    git \
    python3 \
  && rm -rf /var/lib/apt/lists/*

RUN git clone --depth=1 https://github.com/ggml-org/llama.cpp.git /tmp/llama.cpp \
  && cmake -S /tmp/llama.cpp -B /tmp/llama.cpp/build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=OFF \
    -DGGML_NATIVE=OFF \
    -DLLAMA_BUILD_SERVER=ON \
    -DLLAMA_BUILD_EXAMPLES=OFF \
    -DLLAMA_BUILD_TESTS=OFF \
  && cmake --build /tmp/llama.cpp/build --target llama-server -j"$(nproc)"


FROM node:22-bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    bash \
    git \
    ca-certificates \
    libgomp1 \
    chromium \
    fonts-liberation \
    fonts-noto-color-emoji \
  && rm -rf /var/lib/apt/lists/*

RUN pip3 install --break-system-packages --no-cache-dir "psycopg[binary]" redis html2text scrapling openai numpy

ARG OPENCLAW_VERSION
RUN mkdir -p /var/tmp/openclaw-compile-cache \
  && npm install -g "openclaw@${OPENCLAW_VERSION}" node-llama-cpp @aisuite/chub \
  && npm cache clean --force \
  && rm -rf /root/.npm /root/.cache

COPY platform/bin /app/platform/bin
COPY backend /app/backend
COPY platform/deliverables /app/platform/deliverables
COPY platform/config/themes /app/platform/config/themes
COPY platform/skills /app/platform/skills
COPY platform/vendor /app/platform/vendor
COPY --from=frontend-build /app/frontend/dist /app/apps/frontend/dist
RUN find /app/platform/bin -type f \( -name "*.sh" -o -name "*.py" \) -exec chmod +x {} +

ENV OPENCLAW_DIR=/data/openclaw
ENV OPENCLAW_STATE_DIR=/data/openclaw
ENV OPENCLAW_CONFIG_PATH=/data/openclaw/openclaw.json
ENV NODE_COMPILE_CACHE=/var/tmp/openclaw-compile-cache
ENV OPENCLAW_NO_RESPAWN=1
ENV OPENCLAW_ALLOW_RFC2544_BENCHMARK_RANGE=1
ENV PORT=18890

EXPOSE 18890

CMD ["bash", "/app/platform/bin/deploy/docker_bootstrap.sh"]
