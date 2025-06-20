from pydantic import BaseModel, StrictStr, StrictInt, StrictFloat
from typing import Dict


class StationMetaData(BaseModel):
    stationName: StrictStr  # A human-readable string identifying the weather station
    timestamp: StrictInt  # A UTC millisecond precision timestamp representing when the sample was taken, as an integer number. This timestamp is guaranteed to increase in subsequent samples.
    temperature: StrictFloat  # The floating point Fahrenheit temperature


class StationOutputMetaData(BaseModel):
    stationName: StrictStr
    high: StrictFloat
    low: StrictFloat


class StationsMonitor(BaseModel):
    stations: Dict[str, Dict[str, float]] = {}
    
    def __add__(self, other: StationMetaData) -> "StationsMonitor":
        updated = self.stations.copy()
        if other.stationName in updated:
            current = updated[other.stationName]
            current['high'] = max(current['high'], other.temperature)
            current['low'] = min(current['low'], other.temperature)
        else:
            updated[other.stationName] = {
                'high': other.temperature,
                'low' : other.temperature
            }
        return StationsMonitor(stations=updated)
    
    def reset(self) -> None:
        self.stations.clear()
