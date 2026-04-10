#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OUT_DIR="${ROOT_DIR}/local/output/single-tenant-package"

GREEN='\033[0;32m'; NC='\033[0m'
ok() { echo -e "${GREEN}[✓]${NC} $*"; }

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR/docs" "$OUT_DIR/bin/verify"

cp "$ROOT_DIR/.env.single-tenant.example" "$OUT_DIR/"
cp "$ROOT_DIR/.env.providers.example" "$OUT_DIR/"
cp "$ROOT_DIR/docker-compose.yml" "$OUT_DIR/"

cp "$ROOT_DIR/platform/bin/verify/verify_single_tenant_install.sh" "$OUT_DIR/platform/bin/verify/"
cp "$ROOT_DIR/platform/bin/verify/day_one_smoke_check.sh" "$OUT_DIR/platform/bin/verify/"

cp "$ROOT_DIR/docs/single-tenant-install.md" "$OUT_DIR/docs/"
cp "$ROOT_DIR/docs/single-tenant-delivery-checklist.md" "$OUT_DIR/docs/"
cp "$ROOT_DIR/docs/single-tenant-env-guide.md" "$OUT_DIR/docs/"
cp "$ROOT_DIR/docs/single-tenant-troubleshooting.md" "$OUT_DIR/docs/"
cp "$ROOT_DIR/docs/production-deployment-plan.md" "$OUT_DIR/docs/"

cat > "$OUT_DIR/README.txt" <<'EOF'
OpenClaw Team · 单客户单实例交付包

建议顺序：

1. 复制 .env.single-tenant.example 为 .env
2. 参考 .env.providers.example 填写真实密钥
3. 执行 docker compose up -d --build
4. 执行 bash platform/bin/verify/verify_single_tenant_install.sh
5. 首日运行 bash platform/bin/verify/day_one_smoke_check.sh

文档入口：

- docs/single-tenant-install.md
- docs/single-tenant-delivery-checklist.md
- docs/single-tenant-env-guide.md
- docs/single-tenant-troubleshooting.md
EOF

chmod +x "$OUT_DIR/platform/bin/verify/verify_single_tenant_install.sh" "$OUT_DIR/platform/bin/verify/day_one_smoke_check.sh"

ok "单租户交付目录已生成: $OUT_DIR"
