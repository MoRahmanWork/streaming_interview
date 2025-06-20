from typing import Union
from pydantic import BaseModel, Field

from interview.models.sampleEvent import SampleEvent
from interview.models.controlEvent import ControlEvent


class InputEvent(BaseModel):
    event: Union[SampleEvent, ControlEvent] = Field(discriminator="type")
