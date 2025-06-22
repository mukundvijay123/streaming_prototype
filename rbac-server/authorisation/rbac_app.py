import casbin
from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from typing import Optional
from pydantic import BaseModel
from our_secrets import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES
import sqlalchemy_adapter
# Initialize Casbin enforcer with model and policy file
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
adapter = sqlalchemy_adapter.Adapter("sqlite:///idp.db")
db_enforcer = casbin.Enforcer("authorisation/model.conf", adapter)

rbac_app = APIRouter()


class TokenUser(BaseModel):
    sub: str
    user: str


# Decode JWT and extract user info
def get_current_user(token: str = Depends(oauth2_scheme)) -> TokenUser:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: Optional[str] = payload.get("sub")
        user: Optional[str] = payload.get("user")


        if not user_id or not user:
          
            raise HTTPException(status_code=401, detail="Invalid token")

        return TokenUser(sub=user_id, user=user)

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


# Authorization check endpoint
@rbac_app.get("/authorize")
def authorize_access(
    topic: str = Query(),
    action: str = Query(),
    current_user: TokenUser = Depends(get_current_user)
):
    user = current_user.user
    obj = topic
    act = action
    print(f"Checking access for user: {user}, topic: {obj}, action: {act}")
    #get from the db now
    db_enforcer.load_policy()
    if db_enforcer.enforce(user, obj, act):
        return {
            "message": "Access granted",
            "user": user,
            "topic": topic,
            "action": action
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied for role '{user}' on topic '{topic}' with action '{action}'"
        )


        
