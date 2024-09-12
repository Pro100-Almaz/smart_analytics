from pydantic import BaseModel


class Impulse(BaseModel):
    interval: int
    percentage: int

class TickerTracking(BaseModel):
    ticker_name: str
    time_period: int
