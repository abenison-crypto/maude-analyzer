"""MAUDE Analyzer FastAPI Application."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import get_settings
from api.routers import events, analytics, admin

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="API for FDA MAUDE (Manufacturer and User Facility Device Experience) data analysis",
)

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
