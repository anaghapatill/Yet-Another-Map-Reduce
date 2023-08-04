import multiprocessing
import socket
import pickle
import threading
import shufflesort
from filehelper import *
from os import listdir
from os.path import isfile, join
from os import walk
import enums
from messages import *
from task import Task


class MasterProcess(multiprocessing.Process):
    def __init__(self, host, port, path, path_map, path_reduce, path_shuffle, num_of_workers):
        super().__init__()
        self.worker_machines_in_use = []
        self.host = host
        self.port = port
        self.num_of_workers = num_of_workers
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        print("Master started: ", self.host, ", port: ", self.port)
        self.worker_machines = []
        self.path = path
        self.path_map = path_map
        self.path_reduce = path_reduce
        self.path_shuffle = path_shuffle
        self.map_tasks = []
        self.reduce_tasks = []
        self.map_task_finished = False
        self.terminate = False

    def listen(self):
        while not self.terminate:
            data, address = self.sock.recvfrom(1024)
            message = pickle.loads(data)
            if message.message_type == enums.MessageType.SETUP:
                print("Worker connected: ", address)
                self.worker_machines.append((message.ip, message.port))
                response = ResponseMessage(enums.Response.SUCCESSFUL)
                data_string = pickle.dumps(response)
                self.sock.sendto(data_string, address)
                if self.num_of_workers == len(self.worker_machines):
                    self.assign_tasks()
            if message.message_type == enums.MessageType.COMPLETE_TASK:
                self.complete_task(address, message.task)
                self.assign_tasks()
                if message.task == enums.TaskTypes.MAP:
                    if not self.map_task_finished:
                        if self.all_tasks_finished(enums.TaskTypes.MAP):
                            print("SHUFFLE STARTED!")
                            shufflesort.shuffle(self.path_map, self.path_shuffle)
                            print("SHUFFLE FINISHED!")
                            self.reduce_tasks = self.create_tasks(self.path_shuffle, self.path_reduce, enums.TaskTypes.REDUCE)
                            self.assign_tasks()
                if message.task == enums.TaskTypes.REDUCE:
                    if self.all_tasks_finished(enums.TaskTypes.REDUCE):
                        data_reader = DataReader()
                        data_reader.combine_multiple_files(self.path_reduce, "result/")
                        close_worker_message = CloseWorkerProcessMessage()
                        data_string = pickle.dumps(close_worker_message)
                        for worker in self.worker_machines:
                            self.sock.sendto(data_string, (worker[0], worker[1]))
                        self.terminate = True
                        print("Closing " + self.name + "(" + str(self.host) + ":" + str(self.port) + ")")

    def run(self):
        self.map_tasks = self.create_tasks(self.path, self.path_map, enums.TaskTypes.MAP)
        threading.Thread(target=self.listen, args=()).start()

    @staticmethod
    def create_tasks(path_read, path_save, task_type):
        tasks = []
        files = DataReader.read_all_file_names_from_location(path_read)
        for file in files:
            tasks.append(Task(task_type, path_read + file, path_save, enums.State.IDLE, enums.TaskTypes.NONE))
        return tasks

    def assign_tasks(self):
        if not self.map_task_finished:
            for map_task in self.map_tasks:
                if map_task.state == enums.State.IDLE:
                    if map_task.worker == enums.TaskTypes.NONE:
                        for worker in self.worker_machines:
                            if worker not in self.worker_machines_in_use:
                                map_task.worker = worker
                                self.worker_machines_in_use.append(worker)
                                task_message = TaskMessage(enums.TaskTypes.MAP, map_task.path_read, map_task.path_save)
                                data_string = pickle.dumps(task_message)
                                self.sock.sendto(data_string, (worker[0], worker[1]))
                                map_task.state = enums.State.IN_PROGRESS
                                break
        else:
            for reduce_task in self.reduce_tasks:
                if reduce_task.state == enums.State.IDLE:
                    if reduce_task.worker == enums.TaskTypes.NONE:
                        for worker in self.worker_machines:
                            if worker not in self.worker_machines_in_use:
                                reduce_task.worker = worker
                                self.worker_machines_in_use.append(worker)
                                task_message = TaskMessage(enums.TaskTypes.REDUCE, reduce_task.path_read, reduce_task.path_save)
                                data_string = pickle.dumps(task_message)
                                self.sock.sendto(data_string, (worker[0], worker[1]))
                                reduce_task.state = enums.State.IN_PROGRESS
                                break

    def complete_task(self, worker, task_type):
        if task_type == enums.TaskTypes.MAP:
            for map_task in self.map_tasks:
                if map_task.state == enums.State.IN_PROGRESS:
                    if worker == map_task.worker:
                        map_task.state = enums.State.COMPLETED
                        if worker in self.worker_machines_in_use:
                            self.worker_machines_in_use.remove(worker)
        elif task_type == enums.TaskTypes.REDUCE:
            for reduce_task in self.reduce_tasks:
                if reduce_task.state == enums.State.IN_PROGRESS:
                    if worker == reduce_task.worker:
                        reduce_task.state = enums.State.COMPLETED
                        if worker in self.worker_machines_in_use:
                            self.worker_machines_in_use.remove(worker)

    def all_tasks_finished(self, task_type):
        if task_type == enums.TaskTypes.MAP:
            if all(x.state == enums.State.COMPLETED for x in self.map_tasks):
                self.map_task_finished = True
                print("ALL MAP TASKS COMPLETED!")
                return True
        elif task_type == enums.TaskTypes.REDUCE:
            if all(x.state == enums.State.COMPLETED for x in self.reduce_tasks):
                self.map_task_finished = True
                print("ALL REDUCE TASKS COMPLETED!")
                return True
        else:
            return False