from enum import Enum, auto


class EventTypes(Enum):
    sample = auto()
    control = auto()


class CommandTypes(Enum):
    snapshot = auto()
    reset = auto()

