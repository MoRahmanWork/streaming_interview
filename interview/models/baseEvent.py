from pydantic import BaseModel


class BaseEvent(BaseModel):
    type: str  # abstract
