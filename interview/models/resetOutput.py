from typing import Literal
from pydantic import BaseModel, StrictInt


class ResetOutput(BaseModel):
    type: Literal["reset"]  # The output type (reset in this example)
    asOf: StrictInt  # The most recent weather sample timestamp received
    # at the point when the reset occurred. Data received at or before
    # this timestamp should not be included in any subsequent snapshot responses.
