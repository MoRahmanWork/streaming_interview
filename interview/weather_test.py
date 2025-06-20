import time
import unittest
from pydantic import ValidationError

from interview.models.sampleEvent import SampleEvent
from interview.models.controlEvent import ControlEvent
from interview.models.inputEvent import InputEvent
from interview.models.resetOutput import ResetOutput
from interview.models.snapshotOutput import SnapshotOutput
from interview.models.stations import StationsMonitor
from . import weather


class TestWeather(unittest.TestCase):
    
    @staticmethod
    def data():
        return {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
    
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
        assert not stations.stations
        
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
        self.assertEqual(actual_timestamp, expected_timestamp, msg="simple sample timestamp")
        self.assertEqual(actual_stations, expected_stations, msg="simple sample stations")
        
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
        self.assertEqual(actual_timestamp, expected_timestamp, msg="high temp sample timestamp")
        self.assertEqual(actual_stations, expected_stations, msg="high temp sample stations")
        
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
        self.assertEqual(actual_timestamp, expected_timestamp, msg="high temp sample timestamp")
        self.assertEqual(actual_stations, expected_stations, msg="high temp sample stations")
        
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
        self.assertEqual(actual_timestamp, expected_timestamp, msg="high temp sample timestamp")
        self.assertEqual(actual_stations, expected_stations, msg="high temp sample stations")
    
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
        self.assertEqual(actual_snapshot, expected_snapshot)
    
    def test__cmd_generate_reset_output(self):
        timestamp = 1672531200003
        actual_reset = weather._cmd_generate_reset_output(
            timestamp=timestamp
        )
        expected_reset = ResetOutput(
            type='reset',
            asOf=1672531200003
        ).model_dump()
        self.assertEqual(actual_reset, expected_reset)
    
    def test_process_events_sample_deterministic(self):
        weather.stations_montior.reset()
        weather.latest_timestamp = None
        
        sample_data = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
        events = [sample_data]
        gen_pe = weather.process_events(events=events)
        yield_1 = gen_pe.__next__()
        self.assertEqual(yield_1, sample_data)
    
    def test_process_events_sample_stochastic(self):
        weather.stations_montior.reset()
        weather.latest_timestamp = None
        
        sample_data = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
        data_1 = sample_data.copy()
        data_2 = sample_data.copy()
        data_2["timestamp"] = 1672531200001
        data_3 = sample_data.copy()
        data_3["timestamp"] = 1672531200002
        data_4 = sample_data.copy()
        data_4["timestamp"] = 1672531200003
        batch_events = [data_1, data_2, data_3, data_4]
        gen_pe = weather.process_events(events=batch_events)
        for i in range(len(batch_events)):
            yield_data = gen_pe.__next__()
            self.assertEqual(yield_data, batch_events[i], msg="multiple samples -- generator")
        
        actual = list(weather.process_events(events=batch_events))
        expected = batch_events
        self.assertEqual(actual, expected, msg="multiple samples -- list")
    
    def test_performance(self):
        weather.stations_montior.reset()
        weather.latest_timestamp = None
        
        start_time = time.time()
        
        # 1M rows
        # number_of_sample_messages = 1_000_000
        number_of_sample_messages = 10_000
        number_of_sample_messages = 10
        sample_data = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
        
        # Deterministic same station
        events = [sample_data.copy()] * number_of_sample_messages
        output = list(weather.process_events(events=events))
        self.assertEqual(len(output), number_of_sample_messages)
        
        # Deterministic multiple stations
        # 1M rows
        events = [sample_data.copy() for _ in range(number_of_sample_messages)]
        for i in range(number_of_sample_messages):
            current_event = events[i].copy()
            events[i]["stationName"] = current_event["stationName"] + str(i)
        output = list(weather.process_events(events=events))
        self.assertEqual(len(output), number_of_sample_messages)
        
        cmd_snapshot = {"type": "control", "command": "snapshot"}
        new_event = [cmd_snapshot]
        actual = list(weather.process_events(events=new_event))
        expected = [{
            'type'    : 'snapshot',
            'asOf': 1672531200000,
            'stations': {
                'Foster Weather Station' : {'high': 37.1, 'low' : 37.1},
                'Foster Weather Station0': {'high': 37.1, 'low' : 37.1},
                'Foster Weather Station1': {'high': 37.1, 'low' : 37.1},
                'Foster Weather Station2': {'high': 37.1, 'low' : 37.1},
                'Foster Weather Station3': {'high': 37.1, 'low' : 37.1},
                'Foster Weather Station4': {'high': 37.1, 'low' : 37.1},
                'Foster Weather Station5': {'high': 37.1, 'low' : 37.1},
                'Foster Weather Station6': {'high': 37.1, 'low' : 37.1},
                'Foster Weather Station7': {'high': 37.1, 'low' : 37.1},
                'Foster Weather Station8': {'high': 37.1, 'low' : 37.1},
                'Foster Weather Station9': {'high': 37.1, 'low' : 37.1}
            }
        }]
        self.assertEqual(actual, expected)
        
        end_time = time.time()
        print(f"Time taken: {end_time - start_time}")
    
    def test_process_events_validation_error(self):
        # event type error
        sample_data = {
            "type"       : "asd",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
        events = [sample_data]
        with self.assertRaises(ValidationError):
            weather.process_events(events).__next__()
        
        # dtype errors
        # stationName dtype error
        station_dtype = sample_data.copy()
        station_dtype["stationName"] = 1000
        events = [station_dtype]
        with self.assertRaises(ValidationError):
            weather.process_events(events).__next__()
        # timestamp dtype error
        timestamp_dtype = sample_data.copy()
        timestamp_dtype["timestamp"] = "None"
        events = [timestamp_dtype]
        with self.assertRaises(ValidationError):
            weather.process_events(events).__next__()
        # temperature dtype error
        temperature_dtype = sample_data.copy()
        temperature_dtype["temperature"] = "None"
        events = [temperature_dtype]
        with self.assertRaises(ValidationError):
            weather.process_events(events).__next__()
        # command- dtype error
        command_dtype = {"type": "command","command":123}
        events = [command_dtype]
        with self.assertRaises(ValidationError):
            weather.process_events(events).__next__()
    
    def test_process_events_cmd_snapshot(self):
        weather.stations_montior.reset()
        weather.latest_timestamp = None
        
        number_of_sample_messages = 10
        sample_data = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
        events = [sample_data.copy() for _ in range(number_of_sample_messages)]
        for i in range(number_of_sample_messages):
            current_event = events[i].copy()
            events[i]["stationName"] = current_event["stationName"] + str(i)
        output = list(weather.process_events(events=events))
        self.assertEqual(len(output), number_of_sample_messages)
        
        cmd_snapshot = {"type": "control", "command": "snapshot"}
        new_event = [cmd_snapshot]
        actual = list(weather.process_events(events=new_event))
        expected = [{
            'type'    : 'snapshot',
            'asOf'    : 1672531200000,
            'stations': {
                'Foster Weather Station0': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station1': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station2': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station3': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station4': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station5': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station6': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station7': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station8': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station9': {'high': 37.1, 'low': 37.1}
            }
        }]
        self.assertEqual(actual, expected)
        
        # new timestamp and new temperature
        forward_time = {
            "type"       : "sample",
            "stationName": "Foster Weather Station0",
            "timestamp"  : 1672531200011,
            "temperature": 111.1
        }
        cmd_snapshot = {"type": "control", "command": "snapshot"}
        new_event = [forward_time, cmd_snapshot]
        actual = list(weather.process_events(events=new_event))
        actual = [actual[-1]]
        
        expected =[{
            'type': 'snapshot',
            'asOf': 1672531200011,
            'stations': {
                'Foster Weather Station0': {'high': 111.1, 'low': 37.1},
                'Foster Weather Station1': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station2': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station3': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station4': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station5': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station6': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station7': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station8': {'high': 37.1, 'low': 37.1},
                'Foster Weather Station9': {'high': 37.1, 'low': 37.1}
            }
        }]
        self.assertEqual(actual, expected)
    
    def test_process_events_cmd_reset(self):
        weather.stations_montior.reset()
        weather.latest_timestamp = None
        
        number_of_sample_messages = 2
        sample_data = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
        events = [sample_data.copy() for _ in range(number_of_sample_messages)]
        for i in range(number_of_sample_messages):
            current_event = events[i].copy()
            events[i]["stationName"] = current_event["stationName"] + str(i)
        output = list(weather.process_events(events=events))
        self.assertEqual(len(output), number_of_sample_messages)
        
        # reset data
        cmd_reset = {"type": "control", "command": "reset"}
        new_event = [cmd_reset]
        actual = list(weather.process_events(events=new_event))
        expected = [{
            'type'    : 'reset',
            'asOf'    : 1672531200000,
        }]
        self.assertEqual(actual, expected)
        self.assertEqual(weather.stations_montior.stations, {})
        
        # a new reset after reset should be empty.
        cmd_reset = {"type": "control", "command": "reset"}
        new_event = [cmd_reset]
        actual = list(weather.process_events(events=new_event))
        expected = []
        self.assertEqual(actual, expected)
    
    def test_process_events_real_time_simulation(self):
        # Your goal in this interview is to change the program to correctly process
        # these messages, and produce snapshots of the state when requested.
        # The incoming messages types are heterogeneous, and can include both samples
        # from weather stations, and control messages.
        weather.stations_montior.reset()
        weather.latest_timestamp = None
        
        forest_data = {
            "type"       : "sample",
            "stationName": "Foster Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 37.1
        }
        desert_data = {
            "type"       : "sample",
            "stationName": "Desert Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": 100.0
        }
        ocean_data = {
            "type"       : "sample",
            "stationName": "Ocean Weather Station",
            "timestamp"  : 1672531200000,
            "temperature": -30.0
        }
        
        cmd_snapshot = {"type": "control", "command": "snapshot"}
        cmd_reset = {"type": "control", "command": "reset"}
        
        # first entry, base data for three regions stations
        first_entry = [
            forest_data,
            desert_data,
            ocean_data
        ]
        # second entry, increase and decrease temp for next timestamp, and get snapshot cmd
        forest_data2 = forest_data.copy()
        forest_data2["timestamp"] += 10
        forest_data2["temperature"] -= 10
        desert_data2 = desert_data.copy()
        desert_data2["timestamp"] += 10
        desert_data2["temperature"] += 10
        second_entry = [
            forest_data2,
            desert_data2,
            cmd_snapshot
        ]
        # third entry, reset command
        third_entry = [
            cmd_reset
        ]
        # forth entry, decrease temp for next timestamp ocean data and get snapshot
        ocean_data2 = ocean_data.copy()
        ocean_data2["timestamp"] += 100
        ocean_data2["temperature"] -= 10
        forth_entry = [
            ocean_data2,
            cmd_snapshot
        ]
        
        entries = [first_entry, second_entry, third_entry, forth_entry]
        expected = [
            [
                {'type': 'sample',
                 'stationName': 'Foster Weather Station',
                 'timestamp': 1672531200000,
                 'temperature': 37.1},
                {'type'       : 'sample',
                 'stationName': 'Desert Weather Station',
                 'timestamp': 1672531200000,
                 'temperature': 100.0},
                {'type'       : 'sample',
                 'stationName': 'Ocean Weather Station',
                 'timestamp': 1672531200000,
                 'temperature': -30.0}
            ], # first entry
            [
                {'type'       : 'sample',
                 'stationName': 'Foster Weather Station',
                 'timestamp': 1672531200010,
                 'temperature': 27.1},
                {'type'       : 'sample',
                 'stationName': 'Desert Weather Station',
                 'timestamp': 1672531200010,
                 'temperature': 110.0},
                {'type'    : 'snapshot',
                 'asOf': 1672531200010,
                 'stations': {'Foster Weather Station': {'high': 37.1, 'low': 27.1},
                              'Desert Weather Station': {'high': 110.0, 'low': 100.0},
                              'Ocean Weather Station' : {'high': -30.0, 'low': -30.0}}}
            ], # second entry
            [
                {'type': 'reset', 'asOf': 1672531200010}
            ], # third entry
            [
                {'type'       : 'sample',
                 'stationName': 'Ocean Weather Station',
                 'timestamp': 1672531200100,
                 'temperature': -40.0},
                {'type'    : 'snapshot',
                 'asOf': 1672531200100,
                 'stations': {'Ocean Weather Station' : {'high': -40.0, 'low': -40.0}}}
            ], # forth entry
        ]
        
        for i in range(len(entries)):
            generator_pe = weather.process_events(events=entries[i])
            for j in range(len(entries[i])):
                actual = generator_pe.__next__()
                self.assertEqual(actual, expected[i][j])
