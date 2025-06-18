from typing import Any, Iterable, Generator, Union, Literal
from pydantic import ValidationError
from logging import getLogger

from interview.models.sampleEvent import SampleEvent
from interview.models.controlEvent import ControlEvent
from interview.models.inputEvent import InputEvent
from interview.models.resetOutput import ResetOutput
from interview.models.snapshotOutput import SnapshotOutput

outputEvent = Union[SnapshotOutput, ResetOutput]

logger = getLogger(__name__)


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    """
    Process a stream of samples from weather stations on Chicago city beaches into messages
    that provide snapshots of the aggregated state of the weather
    :param events: an Iterable of input dicts {str: Any} messages
    :return: Output messages json dicts {str, Any}
    """
    for line in events:
        yield line
