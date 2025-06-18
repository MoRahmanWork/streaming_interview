from typing import Literal
from pydantic import BaseModel, StrictInt


class SnapshotOutput(BaseModel):
    type: Literal["snapshot"]  # The output type ("snapshot" in this instance)
    asOf: StrictInt  # The most recent weather sample timestamp received at the point when the snapshot or aggregation was taken. Data with a timestamp later than this time should not be included in the output. All data with a timestamp equal to or before this timestamp must be included.
    stations: dict[str, dict]  # A object that uses station names as keys, with object values that contain high and low temperature values
