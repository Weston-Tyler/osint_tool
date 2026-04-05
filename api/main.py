"""MDA Intelligence Database — FastAPI application.

REST + WebSocket API for querying the MDA graph, spatial, and search layers.
"""

import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from gqlalchemy import Memgraph

from api.routers import analytics, uas, vessels


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and tear down shared resources."""
    # Startup
    app.state.memgraph = Memgraph(
        host=os.getenv("MEMGRAPH_HOST", "localhost"),
        port=int(os.getenv("MEMGRAPH_PORT", "7687")),
    )
    app.state.postgres_pool = await asyncpg.create_pool(
        dsn=f"postgresql://{os.getenv('POSTGRES_USER', 'mda')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'mda')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'mda')}",
        min_size=5,
        max_size=20,
    )
    yield
    # Shutdown
    await app.state.postgres_pool.close()


app = FastAPI(
    title="MDA Intelligence Database API",
    description="Maritime Domain Awareness — Open Source Intelligence Database",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in Phase 4 with Keycloak
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(vessels.router, prefix="/v1", tags=["vessels"])
app.include_router(uas.router, prefix="/v1", tags=["uas"])
app.include_router(analytics.router, prefix="/v1", tags=["analytics"])


@app.get("/health")
async def health_check():
    """System health check — all services."""
    health = {"status": "ok", "services": {}}

    # Check Memgraph
    try:
        result = list(app.state.memgraph.execute_and_fetch("RETURN 1 AS n"))
        health["services"]["memgraph"] = "ok" if result else "error"
    except Exception as e:
        health["services"]["memgraph"] = f"error: {e}"
        health["status"] = "degraded"

    # Check PostgreSQL
    try:
        async with app.state.postgres_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        health["services"]["postgres"] = "ok"
    except Exception as e:
        health["services"]["postgres"] = f"error: {e}"
        health["status"] = "degraded"

    return health
