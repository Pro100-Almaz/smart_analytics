from datetime import datetime, timedelta

from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, OAuth2PasswordBearer
from dotenv import load_dotenv

import jwt
import os


load_dotenv()

SECRET_KEY = os.getenv('SECRET_KEY')
ALGORITHM = os.getenv('ALGORITHM')
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', 5))


# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")
#
# def get_current_user(token: str = Depends(oauth2_scheme)):
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         telegram_id: str = payload.get("telegram_id")
#         if telegram_id is None:
#             raise HTTPException(status_code=403, detail="Invalid token or expired token.")
#         return {"telegram_id": telegram_id, "username": payload.get("username")}
#     except jwt.PyJWTError:
#         raise HTTPException(
#             status_code=403,
#             detail="Could not validate credentials",
#         )


def verify_jwt(jw_token: str) -> bool:
    try:
        payload = jwt.decode(jw_token, SECRET_KEY, algorithms=[ALGORITHM])
        expire = datetime.utcfromtimestamp(payload.get("exp"))

        if expire < datetime.utcnow():
            return False
        return payload

    except:
        return False


class JWTBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials = await super(JWTBearer, self).__call__(request)
        if credentials:
            data = verify_jwt(credentials.credentials)
            if not credentials.scheme == "Bearer":
                raise HTTPException(status_code=403, detail="Invalid authentication scheme.")
            if not data:
                raise HTTPException(status_code=403, detail="Invalid token or expired token.")
            return data
        else:
            raise HTTPException(status_code=403, detail="Invalid authorization code.")
