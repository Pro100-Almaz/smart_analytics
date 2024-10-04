from pydantic import BaseModel


class VolumeData(BaseModel):
    active_name: str
    time_value: int
