from typing import Literal
from pydantic import StrictStr

from interview.models.baseEvent import BaseEvent


class ControlEvent(BaseEvent):
    type: Literal["control"]
    command: StrictStr
