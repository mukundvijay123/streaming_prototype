from sqlalchemy import Column, String, Boolean, DateTime,Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import uuid

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    full_name = Column(String, nullable=True)
    role = Column(String, default="user")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())

class Stream(Base):
    __tablename__="stream"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    topic = Column(String, unique=True, index=True, nullable=False)
