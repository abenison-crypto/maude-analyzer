"""MAUDE Analyzer FastAPI Application."""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware

from api.config import get_settings
from api.routers import events, analytics, admin, data_quality

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="API for FDA MAUDE (Manufacturer and User Facility Device Experience) data analysis",
)


class CacheHeaderMiddleware(BaseHTTPMiddleware):
    """Middleware to add Cache-Control headers to responses."""

    # Endpoints that can be cached
    CACHEABLE_PATHS = {
        "/api/events/manufacturers": 1800,  # 30 minutes
        "/api/events/product-codes": 1800,  # 30 minutes
        "/api/events/stats": 300,  # 5 minutes
        "/api/analytics/trends": 600,  # 10 minutes
        "/api/analytics/event-type-distribution": 600,  # 10 minutes
        "/api/admin/status": 60,  # 1 minute
    }

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        # Only cache GET requests
        if request.method != "GET":
            return response

        # Check if this path should be cached
        path = request.url.path
        for cacheable_path, max_age in self.CACHEABLE_PATHS.items():
            if path.startswith(cacheable_path):
                response.headers["Cache-Control"] = f"public, max-age={max_age}"
                break
        else:
            # Default: no cache for other endpoints
            response.headers["Cache-Control"] = "no-cache"

        return response


# Add cache header middleware
app.add_middleware(CacheHeaderMiddleware)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(events.router, prefix="/api/events", tags=["Events"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])
app.include_router(admin.router, prefix="/api/admin", tags=["Admin"])
app.include_router(data_quality.router, prefix="/api", tags=["Data Quality"])


@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "name": settings.app_name,
        "version": settings.version,
        "docs": "/docs",
        "endpoints": {
            "events": "/api/events",
            "analytics": "/api/analytics",
            "admin": "/api/admin",
            "data_quality": "/api/data-quality",
        },
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    from api.services.database import get_db

    try:
        db = get_db()
        count = db.execute("SELECT COUNT(*) FROM master_events").fetchone()[0]
        return {
            "status": "healthy",
            "database": "connected",
            "total_events": count,
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
        }


# Serve static frontend files in production
import os
from pathlib import Path

STATIC_DIR = Path(__file__).parent.parent / "static"

if STATIC_DIR.exists():
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse

    # Mount assets directory for JS, CSS, images
    assets_dir = STATIC_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        """Serve frontend for all non-API routes."""
        # Check if file exists in static dir
        file_path = STATIC_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)

        # Return index.html for SPA routing
        index_path = STATIC_DIR / "index.html"
        if index_path.exists():
            return FileResponse(index_path)

        return {"detail": "Not found"}
