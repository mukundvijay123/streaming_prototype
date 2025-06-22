from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "sqlite+aiosqlite:///./idp.db"

# Create the async engine
engine = create_async_engine(DATABASE_URL, echo=True)

# Create a sessionmaker factory
async_sessionmaker = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Dependency to get DB session
async def get_db():
    async with async_sessionmaker() as session:
        yield session
