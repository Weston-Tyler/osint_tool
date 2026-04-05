# Security Policy

## Supported Versions

We provide security updates for the latest release only.

| Version | Supported          |
|---------|--------------------|
| latest  | Yes                |
| < latest| No                 |

## Reporting a Vulnerability

**DO NOT** file a public GitHub issue for security vulnerabilities.

Instead, please report them via GitHub's private vulnerability reporting:
1. Go to the Security tab of this repository
2. Click "Report a vulnerability"
3. Fill in the details

Include:
1. Description of the vulnerability
2. Affected component(s) and version
3. Steps to reproduce
4. Potential impact assessment
5. Your suggested fix (optional)

We will acknowledge receipt within 48 hours and provide a remediation timeline within 7 days.

## Responsible Disclosure Policy

- We request 90 days to address critical vulnerabilities before public disclosure
- We will credit reporters in the changelog unless they prefer anonymity
- We will not pursue legal action against good-faith security researchers

## Data Handling Considerations

This project handles intelligence data that may be operationally sensitive. If you discover a vulnerability that could expose real intelligence data to unauthorized access, treat it as critical and follow accelerated disclosure procedures.

### Secrets Management

- All credentials must be stored in `.env` files (never committed) or Docker Secrets
- API keys for external services (GFW, AISHub) are injected via environment variables
- No hardcoded passwords or API keys in source code
- Phase 4 will migrate to HashiCorp Vault for production secrets

### Network Security

- Internal service communication uses the Docker bridge network (not exposed to host by default)
- Only the API (8000), Grafana (3001), and management UIs should be exposed externally
- TLS termination should be configured via reverse proxy (nginx/traefik) in production
- Memgraph Bolt (7687) and PostgreSQL (5432) should never be exposed to the internet

## Out of Scope

- Vulnerabilities in third-party Docker images (report to upstream maintainers)
- Denial of service attacks against development instances
- Social engineering
