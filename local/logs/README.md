# Host Product Logs

This directory contains host-mode runtime logs and process metadata.

## Common Files

- `host-product-18890.log`
- `host-product-18891.log`
- `host-product-<port>.pid`
- `config-health.json`

## Notes

- These files help debug host-mode startup, readiness, and config drift.
- Do not delete active pid files while the matching process is still running.
- Historical logs can usually be rotated or archived when they are no longer needed.
