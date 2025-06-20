from datetime import datetime
from typing import Literal
from pydantic import StrictStr, StrictFloat, StrictInt

from interview.models.baseEvent import BaseEvent


class SampleEvent(BaseEvent):
    type: Literal["sample"]  # The message type, as a string. "sample" for weather samples
    stationName: StrictStr  # A human-readable string identifying the weather station
    timestamp: StrictInt  # A UTC millisecond precision timestamp representing
    # when the sample was taken, as an integer number.
    # This timestamp is guaranteed to increase in subsequent samples.
    temperature: StrictFloat  # The floating point Fahrenheit temperature
    
    def timestamp_as_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp / 1000)
