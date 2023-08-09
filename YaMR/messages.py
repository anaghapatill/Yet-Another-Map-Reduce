import enums


class Message:
    def __init__(self, ip, port, name):
        self.name = name
        self.ip = ip
        self.port = port
        self.task_type = enums.TaskTypes.NONE
        self.state = enums.State.IDLE
        self.message_type = enums.MessageType.SETUP


class ResponseMessage:
    def __init__(self, response):
        self.response = response


class TaskMessage:
    def __init__(self, task, path_read, path_save):
        self.task = task
        self.path_read = path_read
        self.path_save = path_save
        self.message_type = enums.MessageType.ASSIGN_TASK


class CompleteTaskMessage:
    def __init__(self, task):
        self.task = task
        self.message_type = enums.MessageType.COMPLETE_TASK


class CloseWorkerProcessMessage:
    def __init__(self):
        self.message_type = enums.MessageType.CLOSE_WORKER_PROCESS
