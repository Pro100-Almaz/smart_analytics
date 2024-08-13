from pydantic import BaseModel


class Authorization(BaseModel):
    data_check_string: str