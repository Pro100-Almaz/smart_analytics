from pydantic import BaseModel


class Authorization(BaseModel):
    data_check_string: str


class Notification(BaseModel):
    last_impulse: bool
    tracking_ticker: bool
