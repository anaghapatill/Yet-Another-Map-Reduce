from enum import Enum


class TaskTypes(Enum):
    MAP = 1
    REDUCE = 2
    NONE = 3


class State(Enum):
    IDLE = 1
    IN_PROGRESS = 2
    COMPLETED = 3


class Response(Enum):
    SUCCESSFUL = 1
    UNSUCCESSFUL = 2


class MessageType(Enum):
    SETUP = 1
    ASSIGN_TASK = 2
    COMPLETE_TASK = 3
    CLOSE_WORKER_PROCESS = 4
