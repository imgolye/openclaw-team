#!/bin/sh
set -euo pipefail

PGHOST=postgres
PGDATABASE="${POSTGRES_DB:-mission_control}"
PGUSER="${POSTGRES_USER:-mission_control}"
export PGPASSWORD="${POSTGRES_PASSWORD:-mission_control}"
BACKUP_DIR="/backups"
KEEP_DAYS="${BACKUP_KEEP_DAYS:-7}"
S3_BUCKET="${S3_BUCKET:-}"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
FILENAME="${PGDATABASE}_${TIMESTAMP}.sql.gz"

# Full backup
pg_dump -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" --no-owner --no-privileges | gzip > "${BACKUP_DIR}/${FILENAME}"

# Optional: upload to S3
if [ -n "$S3_BUCKET" ]; then
  aws s3 cp "${BACKUP_DIR}/${FILENAME}" "s3://${S3_BUCKET}/postgres-backups/${FILENAME}" 2>/dev/null || true
fi

# Cleanup old backups
find "$BACKUP_DIR" -name "${PGDATABASE}_*.sql.gz" -mtime +${KEEP_DAYS} -delete 2>/dev/null || true

echo "[$(date -Iseconds)] Backup complete: ${FILENAME} ($(du -h "${BACKUP_DIR}/${FILENAME}" | cut -f1))"
