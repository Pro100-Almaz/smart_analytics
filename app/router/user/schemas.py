from pydantic import BaseModel


class User(BaseModel):
    data: dict