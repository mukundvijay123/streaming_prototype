from fastapi import FastAPI
from authentication.auth_app import auth_router
from db import engine
from models import Base


app = FastAPI()

app.include_router(auth_router, prefix="/auth")

@app.on_event("startup")
async def startup():

    print("Running startup - creating tables...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)