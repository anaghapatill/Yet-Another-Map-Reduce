import multiprocessing
import socket
import pickle
import threading

import enums
from messages import *
from mapreduce import run


class WorkerProcess(multiprocessing.Process):
    def __init__(self, name, host, port, server_host, server_port):
        super().__init__()
        self.name = name
        self.host = host
        self.port = port
        self.is_connected = False
        self.server_host = server_host
        self.server_port = server_port
        self.server = (self.server_host, self.server_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.terminate = False
        print(name, "was created, ip: ", self.host, "port: ", self.port)

    def connect(self):
        message = Message(self.host, self.port, self.name)
        data_string = pickle.dumps(message)

        while not self.terminate:
            if not self.is_connected:
                self.sock.sendto(data_string, self.server)
                data, address = self.sock.recvfrom(1024)
                response = pickle.loads(data)
                if response.response.SUCCESSFUL:
                    self.is_connected = True
                    print("Good job: ", response.response)
                    threading.Thread(target=self.listen, args=()).start()
            if self.terminate:
                break

    def run(self):
        self.connect()

    def listen(self):
        while not self.terminate:
            data, address = self.sock.recvfrom(1024)
            message = pickle.loads(data)
            if message.message_type == enums.MessageType.ASSIGN_TASK:
                if message.task == enums.TaskTypes.MAP:
                    print(self.name + "(" + str(self.host) + ":" + str(self.port) + ")", message.task, message.path_read)
                    run(["MAP", message.path_read, message.path_save])
                    complete_task = CompleteTaskMessage(enums.TaskTypes.MAP)
                    data_string = pickle.dumps(complete_task)
                    self.sock.sendto(data_string, self.server)
                if message.task == enums.TaskTypes.REDUCE:
                    print(self.name + "(" + str(self.host) + ":" + str(self.port) + ")", message.task, message.path_read)
                    run(["REDUCE", message.path_read, message.path_save])
                    complete_task = CompleteTaskMessage(enums.TaskTypes.REDUCE)
                    data_string = pickle.dumps(complete_task)
                    self.sock.sendto(data_string, self.server)
            elif message.message_type == enums.MessageType.CLOSE_WORKER_PROCESS:
                print("Closing " + self.name + "(" + str(self.host) + ":" + str(self.port) + ")", message.message_type)
                self.terminate = True
