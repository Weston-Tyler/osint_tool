"""Keycloak OAuth2/OIDC authentication and RBAC middleware.

Validates JWT tokens issued by Keycloak, extracts roles, and enforces
role-based access control on API endpoints.

Roles:
- mda-admin: Full access to all endpoints and write operations
- mda-analyst: Read access to all data, write access to intelligence reports
- mda-operator: Read access to operational data (vessels, UAS, alerts)
- mda-viewer: Read-only access to non-sensitive endpoints
"""

import logging
import os
from datetime import datetime
from functools import lru_cache
from typing import Optional

import httpx
from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger("mda.auth")

KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8443")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "mda")
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "false").lower() == "true"

security_scheme = HTTPBearer(auto_error=False)

# Role hierarchy: higher roles inherit lower role permissions
ROLE_HIERARCHY = {
    "mda-admin": {"mda-admin", "mda-analyst", "mda-operator", "mda-viewer"},
    "mda-analyst": {"mda-analyst", "mda-operator", "mda-viewer"},
    "mda-operator": {"mda-operator", "mda-viewer"},
    "mda-viewer": {"mda-viewer"},
}

# Endpoint -> minimum required role
ENDPOINT_ROLES = {
    # Public
    "/health": None,
    # Viewer level
    "/v1/vessels": "mda-viewer",
    "/v1/uas/detections": "mda-viewer",
    "/v1/analytics/risk-summary": "mda-viewer",
    # Operator level
    "/v1/vessels/{identifier}": "mda-operator",
    "/v1/vessels/{identifier}/network": "mda-operator",
    "/v1/vessels/{identifier}/track": "mda-operator",
    "/v1/uas/flight-paths": "mda-operator",
    "/v1/uas/sensors": "mda-operator",
    "/v1/ws/alerts": "mda-operator",
    # Analyst level
    "/v1/analytics/sanctioned-network": "mda-analyst",
    "/v1/analytics/interdiction-heatmap": "mda-analyst",
    "/v1/analytics/ais-gaps-summary": "mda-analyst",
    "/v1/analytics/community-detection": "mda-analyst",
}


class TokenPayload:
    """Parsed JWT token payload."""

    def __init__(self, payload: dict):
        self.sub: str = payload.get("sub", "")
        self.preferred_username: str = payload.get("preferred_username", "anonymous")
        self.email: str = payload.get("email", "")
        self.name: str = payload.get("name", "")
        self.realm_roles: list[str] = payload.get("realm_access", {}).get("roles", [])
        self.client_roles: list[str] = (
            payload.get("resource_access", {}).get("mda-api", {}).get("roles", [])
        )
        self.roles: set[str] = set(self.realm_roles + self.client_roles)
        self.exp: int = payload.get("exp", 0)
        self.iat: int = payload.get("iat", 0)

    @property
    def effective_roles(self) -> set[str]:
        """Get all effective roles including inherited ones."""
        effective = set()
        for role in self.roles:
            effective.update(ROLE_HIERARCHY.get(role, {role}))
        return effective

    def has_role(self, required_role: str) -> bool:
        """Check if user has the required role (including hierarchy)."""
        return required_role in self.effective_roles

    @property
    def is_expired(self) -> bool:
        return datetime.utcnow().timestamp() > self.exp


@lru_cache(maxsize=1)
def _get_jwks_client():
    """Fetch JWKS from Keycloak for token verification."""
    try:
        import jwt
        from jwt import PyJWKClient
        jwks_url = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"
        return PyJWKClient(jwks_url)
    except ImportError:
        logger.warning("PyJWT not installed — token verification disabled")
        return None


def _verify_token(token: str) -> dict:
    """Verify and decode a JWT token using Keycloak's JWKS."""
    try:
        import jwt as pyjwt
    except ImportError:
        # If PyJWT not installed, decode without verification (dev only)
        import json
        import base64
        parts = token.split(".")
        if len(parts) != 3:
            raise HTTPException(status_code=401, detail="Invalid token format")
        payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
        return json.loads(base64.urlsafe_b64decode(payload))

    jwks_client = _get_jwks_client()
    if not jwks_client:
        raise HTTPException(status_code=503, detail="Auth service unavailable")

    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        payload = pyjwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience="mda-api",
            issuer=f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}",
        )
        return payload
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except pyjwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security_scheme),
) -> Optional[TokenPayload]:
    """Extract and validate the current user from the Authorization header.

    Returns None if auth is disabled (development mode).
    """
    if not AUTH_ENABLED:
        return None

    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = _verify_token(credentials.credentials)
    token = TokenPayload(payload)

    if token.is_expired:
        raise HTTPException(status_code=401, detail="Token expired")

    return token


def require_role(required_role: str):
    """FastAPI dependency that enforces a minimum role."""

    async def _check_role(
        user: Optional[TokenPayload] = Depends(get_current_user),
    ):
        if not AUTH_ENABLED:
            return None

        if user is None:
            raise HTTPException(status_code=401, detail="Authentication required")

        if not user.has_role(required_role):
            raise HTTPException(
                status_code=403,
                detail=f"Insufficient permissions. Required role: {required_role}",
            )
        return user

    return _check_role


# Convenience dependencies for common role checks
require_admin = require_role("mda-admin")
require_analyst = require_role("mda-analyst")
require_operator = require_role("mda-operator")
require_viewer = require_role("mda-viewer")
