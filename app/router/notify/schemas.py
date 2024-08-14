from pydantic import BaseModel


class Impulse(BaseModel):
    interval: int
    percentage: int