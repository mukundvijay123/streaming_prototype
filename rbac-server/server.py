from fastapi import FastAPI
from authentication.auth_app import auth_router
from authorisation.rbac_app import rbac_app
from authentication.db import engine
from authentication.models import Base

app = FastAPI(title="RBAC Demo System", version="1.0.0")

app.include_router(auth_router, prefix="/auth", tags=["Authentication"])
app.include_router(rbac_app, prefix="/check", tags=["Authorization"])

@app.on_event("startup")
async def startup():
    print("🚀 Starting RBAC Demo System...")
    print("📊 Creating database tables...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Database setup complete!")

@app.get("/")
async def root():
    return {
        "message": "RBAC Demo System", 
        "docs": "/docs",
        "auth_endpoints": ["/auth/register", "/auth/token", "/auth/me"],
        "rbac_endpoints": ["/check/authorize"]
    }