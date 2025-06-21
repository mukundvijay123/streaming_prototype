from fastapi import FastAPI
from authentication.auth_app import auth_router
from authorisation.rbac_app import rbac_app
from fastapi.middleware.cors import CORSMiddleware
from db import engine
from models import Base
import uvicorn

app = FastAPI()
origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,           # or ["*"] for all (not recommended in prod)
    allow_credentials=True,
    allow_methods=["*"],             # or specify: ["GET", "POST", "OPTIONS", "PUT"]
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/auth")
app.include_router(rbac_app, prefix="/check")


@app.on_event("startup")
async def startup():
    print("Running startup - creating tables...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# Only run when executed directly, not when imported as a module
if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8081, reload=True)
