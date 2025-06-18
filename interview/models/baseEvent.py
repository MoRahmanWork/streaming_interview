from pydantic import BaseModel


class BaseEvent(BaseModel):
    type: str  # abstract
    
    model_config = {
        "discriminator": "type"
    }
