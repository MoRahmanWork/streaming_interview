from . import weather
import pytest
import unittest
from typing import Union
from pydantic import ValidationError

from interview.models.sampleEvent import SampleEvent
from interview.models.controlEvent import ControlEvent
from interview.models.inputEvent import InputEvent
from interview.models.resetOutput import ResetOutput
from interview.models.snapshotOutput import SnapshotOutput


class TestWeather(unittest.TestCase):
    
    def test_model_input_sample(self):
        sample_data = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
        actual = SampleEvent.model_validate(sample_data)
        actual = actual.model_dump()
        expected = sample_data
        self.assertDictEqual(actual, expected)
    
    def test_model_input_control(self):
        control_data = {
            "type"   : "control",
            "command": "snapshot"
        }
        actual = ControlEvent.model_validate(control_data)
        actual = actual.model_dump()
        expected = control_data
        self.assertDictEqual(actual, expected)
    
    def test_model_output_snapshot(self):
        snapshot_data = {
            "type"    : "snapshot",
            "asOf"    : 1672531200000,
            "stations": {
                "Foster Weather Station": {"high": 37.1, "low": 32.5}
            }
        }
        actual = SnapshotOutput.model_validate(snapshot_data)
        actual = actual.model_dump()
        expected = snapshot_data
        self.assertDictEqual(actual, expected)
    
    def test_model_output_reset(self):
        reset_data = {
          "type": "reset",
          "asOf": 1672531200000
        }
        actual = ResetOutput.model_validate(reset_data)
        actual = actual.model_dump()
        expected = reset_data
        self.assertDictEqual(actual, expected)
    
    def test_model_input_errors(self):
        error_data = {
            "event": {
                "type": "None"
            }
        }
        
        with self.assertRaises(ValidationError):
            InputEvent.model_validate(error_data)
        
        err_list = [
            {"event": {"type": "None"}},  # Wrong string
            {"event": {"type": None}},  # Wrong dtype None
            {"event": {"type": 123}},  # Wrong dtype integer
            {"event": {"type": "control"}}  # Not all required fields filled
        ]
        for error_data in err_list:
            with self.assertRaises(ValidationError):
                InputEvent.model_validate(error_data)
    
    def test_process_events(self):
        sample_data = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
        events = [sample_data]
        list(weather.process_events(events=events))
        # TODO -- process events

