from typing import (
    Any, Dict, Iterable, Generator,
    Union, Literal, Optional, Tuple)
from pydantic import ValidationError
from logging import getLogger
import json

from interview.models.sampleEvent import SampleEvent
from interview.models.controlEvent import ControlEvent
from interview.models.inputEvent import InputEvent
from interview.models.resetOutput import ResetOutput
from interview.models.snapshotOutput import SnapshotOutput
from interview.models.eventTypes import EventTypes, CommandTypes
from interview.models.stations import StationsMonitor, StationMetaData, StationOutputMetaData


outputEvent = Union[SnapshotOutput, ResetOutput]

logger = getLogger(__name__)


def _process_samples(sample_msg: SampleEvent,
                     stations: StationsMonitor) -> Tuple[StationsMonitor, int]:
    """
    process sample messages and Update stations tracker and the timestamp tracker
    :param sample_msg:
    :param stations:
    :return:
    """
    stations = stations + StationMetaData(**sample_msg.model_dump())
    return stations, sample_msg.timestamp


def _cmd_generate_snapshot_output(stations: StationsMonitor, timestamp: int) -> Dict[str, Any]:
    """
    Command to generate snapshot output
    :param stations:
    :param timestamp:
    :return:
    """
    output = SnapshotOutput(
        type="snapshot",
        asOf=timestamp,
        stations=stations.stations
    )
    return output.model_dump()


def _cmd_generate_reset_output(timestamp: int) -> Dict[str, Any]:
    """
    Command to geneate reset output
    :param timestamp:
    :return:
    """
    output = ResetOutput(
        type="reset",
        asOf=timestamp
    )
    return output.model_dump()


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    """
    Process a stream of samples from weather stations on Chicago city beaches into messages
    that provide snapshots of the aggregated state of the weather
    :param events: an Iterable of input dicts {str: Any} messages
    :return: Output messages json dicts {str, Any}
    """
    # Initialize
    stations: StationsMonitor = StationsMonitor()  # monitor high/low temp per station
    latest_timestamp: Optional[int] = None  # Monitor timestamp
    
    for line in events:
        data = {"event": line}
        
        try:
            # Validate Model
            line_event = InputEvent.model_validate(data)
        except ValidationError as ve:
            err_msg = f"Validation Error: {ve}"
            logger.critical(err_msg)
            # Covers Unknown Event Types and Unknown Control Types w/ Literal reqs,
            # Covers required missing values.
            # Raise Error -- very informative as Pydantic Validation Error for all fields.
            raise ValidationError(err_msg)
        
        # Obtain the Standardized Event Object
        msg = line_event.event
        # Process Event
        match msg.type:
            case EventTypes.sample.name:
                # Process Sample Events
                print("Sample")
                stations, latest_timestamp = _process_samples(msg, stations)
                yield line
            case EventTypes.control.name:
                # Process Control Events
                print("control")
                match msg.command:
                    case CommandTypes.snapshot.name:
                        # Process Snapshot Commands
                        print("snapshot")
                        if stations and latest_timestamp is not None:
                            yield _cmd_generate_snapshot_output(stations, latest_timestamp)
                    case CommandTypes.reset.name:
                        # Process Reset Commands
                        print("reset")
                        yield _cmd_generate_reset_output(latest_timestamp)
                        stations.reset()
                        latest_timestamp = None
                    case _:
                        # Can't process the Command type
                        # -- Type is added to InputEvent, but no logic handle implemented
                        print("Not implemented Command Type")
            case _:
                # Can't process the Event type
                # - Type added to InputEvent, but no logic handle implemented
                print("Not implemented Event Type")
