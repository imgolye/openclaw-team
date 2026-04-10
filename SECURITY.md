# Security

## Reporting Vulnerabilities

If you discover a security vulnerability, please report it privately via GitHub Security Advisories or email. Do not open a public issue.

## Security Model

OpenClaw Team is designed for self-hosted deployment. Key security features:

### Authentication
- PBKDF2-HMAC-SHA256 password hashing with 600,000 iterations (OWASP 2024)
- 16-byte cryptographically random salt per password
- Timing-safe password comparison
- Session cookies with HttpOnly and SameSite=Lax flags
- CSRF protection via action tokens

### Authorization
- Three-tier role model: Owner / Operator / Viewer
- Permission checks on every API endpoint
- Tenant-scoped API keys for external integrations

### Data Protection
- All data stays on your infrastructure
- No external telemetry or analytics
- API keys provided via environment variables, never hardcoded
- Sensitive files excluded from version control

### API Security
- Parameterized SQL queries (no string interpolation)
- No shell=True in subprocess calls
- Path traversal protection with symlink escape detection
- X-Content-Type-Options and X-Frame-Options headers

## Deployment Checklist

Before deploying to production:

1. Set strong passwords via environment variables:
   - `MISSION_CONTROL_OWNER_PASSWORD`
   - `MISSION_CONTROL_OPERATOR_PASSWORD`
   - `MISSION_CONTROL_VIEWER_PASSWORD`
2. Set `POSTGRES_PASSWORD` to a strong random value
3. Configure CORS origins for your domain (do not use localhost defaults)
4. Use HTTPS with a valid certificate
5. Restrict network access to the management port (18890)
6. Review and rotate API keys regularly
