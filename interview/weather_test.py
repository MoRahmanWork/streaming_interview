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
from interview.models.stations import StationsMonitor, StationMetaData, StationOutputMetaData


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
    
    def test__process_samples(self):
        stations = StationsMonitor()
        assert stations.stations == {}
        
        # test a simple sample input
        sample_data = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
        sample = SampleEvent(**sample_data)
        
        simple_stations, simple_timestamp = weather._process_samples(
            sample_msg=sample, stations=stations
        )
        actual_stations, actual_timestamp = simple_stations, simple_timestamp
        expected_stations = StationsMonitor(
            stations={'Foster Weather Station': {'high': 37.1, 'low': 37.1}}
        )
        expected_timestamp = 1672531200000
        self.assertEquals(actual_timestamp, expected_timestamp, msg="simple sample timestamp")
        self.assertEquals(actual_stations, expected_stations, msg="simple sample stations")
        
        # test a new high temp sample same station
        new_data_high_temp = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200001,
            "temperature": 3700.1
        }
        sample_high_temp = SampleEvent(**new_data_high_temp)
        high_temp_stations, high_temp_timestamp = weather._process_samples(
            sample_msg=sample_high_temp, stations=simple_stations
        )
        actual_stations, actual_timestamp = high_temp_stations, high_temp_timestamp
        expected_stations = StationsMonitor(
            stations={'Foster Weather Station': {'high': 3700.1, 'low': 37.1}}
        )
        expected_timestamp = 1672531200001
        self.assertEquals(actual_timestamp, expected_timestamp, msg="high temp sample timestamp")
        self.assertEquals(actual_stations, expected_stations, msg="high temp sample stations")
        
        # test a new low temp sample same station
        new_data_low_temp = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200002,
            "temperature": -0.1
        }
        sample_low_temp = SampleEvent(**new_data_low_temp)
        low_temp_stations, low_temp_timestamp = weather._process_samples(
            sample_msg=sample_low_temp, stations=simple_stations
        )
        actual_stations, actual_timestamp = low_temp_stations, low_temp_timestamp
        expected_stations = StationsMonitor(
            stations={'Foster Weather Station': {'high': 3700.1, 'low': -0.1}}
        )
        expected_timestamp = 1672531200002
        self.assertEquals(actual_timestamp, expected_timestamp, msg="high temp sample timestamp")
        self.assertEquals(actual_stations, expected_stations, msg="high temp sample stations")
        
        # test a new sample station
        new_data_low_temp = {
            "type"       : "sample",
            "stationName": "Beckton Weather Station",
            "timestamp"  : 1672531200003,
            "temperature": 50.0
        }
        sample_low_temp = SampleEvent(**new_data_low_temp)
        low_temp_stations, low_temp_timestamp = weather._process_samples(
            sample_msg=sample_low_temp, stations=simple_stations
        )
        actual_stations, actual_timestamp = low_temp_stations, low_temp_timestamp
        expected_stations = StationsMonitor(
            stations={'Foster Weather Station': {'high': 3700.1, 'low': -0.1},
                      'Beckton Weather Station': {'high': 50.0, 'low': 50.0}}
        )
        expected_timestamp = 1672531200003
        self.assertEquals(actual_timestamp, expected_timestamp, msg="high temp sample timestamp")
        self.assertEquals(actual_stations, expected_stations, msg="high temp sample stations")
    
    def test__cmd_generate_snapshot_output(self):
        stations = StationsMonitor(
            stations={'Foster Weather Station' : {'high': 3700.1, 'low': -0.1},
                      'Beckton Weather Station': {'high': 50.0, 'low': 50.0}}
        )
        timestamp = 1672531200003
        actual_snapshot = weather._cmd_generate_snapshot_output(
            stations=stations, timestamp=timestamp
        )
        expected_snapshot = SnapshotOutput(
            type='snapshot',
            asOf=1672531200003,
            stations={'Foster Weather Station' : {'high': 3700.1, 'low': -0.1},
                      'Beckton Weather Station': {'high': 50.0, 'low': 50.0}}
        ).model_dump()
        self.assertEquals(actual_snapshot, expected_snapshot)
    
    def test__cmd_generate_reset_output(self):
        timestamp = 1672531200003
        actual_reset = weather._cmd_generate_reset_output(
            timestamp=timestamp
        )
        expected_reset = ResetOutput(
            type='reset',
            asOf=1672531200003
        ).model_dump()
        self.assertEquals(actual_reset, expected_reset)
    
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

