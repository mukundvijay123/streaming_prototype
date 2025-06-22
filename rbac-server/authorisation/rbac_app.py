import casbin
from fastapi import APIRouter, Depends, HTTPException, status,Query
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from typing import Optional
from pydantic import BaseModel
from our_secrets import SECRET_KEY,ALGORITHM,ACCESS_TOKEN_EXPIRE_MINUTES


enforcer = casbin.Enforcer("authorisation\model.conf", "authorisation\policy.csv")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

rbac_app=APIRouter()


class TokenUser(BaseModel):
    sub: str
    role: str

def get_current_user(token: str = Depends(oauth2_scheme)) -> TokenUser:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: Optional[str] = payload.get("sub")
        role: Optional[str] = payload.get("role")

        if not user_id :
            raise HTTPException(status_code=401, detail="Invalid token")
        
        return TokenUser(sub=user_id, role=role)

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    


@rbac_app.get("/authorize")
def authorize_access(
    topic: str = Query(),
    action: str = Query(),
    current_user: TokenUser = Depends(get_current_user)
):
    role = current_user.role
    obj = topic  # The resource, e.g., a Kafka stream or similar
    act = action  # The action the user wants to perform

    if enforcer.enforce(role, obj, act):
        return {
            "message": f"Access granted",
            "role": role,
            "topic": topic,
            "action": action
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f" Access denied for role '{role}' on topic '{topic}' with action '{action}'"
        )


