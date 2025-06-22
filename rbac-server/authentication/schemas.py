from pydantic import BaseModel, EmailStr
from typing import Optional

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    full_name: Optional[str] = None
    role: Optional[str] = "user"

class UserRead(BaseModel):
    id: str
    email: EmailStr
    full_name: Optional[str]
    role: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str
