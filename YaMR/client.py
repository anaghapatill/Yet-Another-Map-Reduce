import os
import multiprocessing
from master import *
from worker import *
from filehelper import DataReader
import sys
import time

M = int(sys.argv[1])
R = M
inputpath = sys.argv[2]
mapperfile = sys.argv[3]
reducerfile = sys.argv[4]

def opt2(inputpath):
    data_reader = DataReader()
    data_reader.open_file(inputpath,'r')
    partition = data_reader.split_file_by_lines()
    data_reader.close_file()

    host = '127.0.0.1'  # client/server ip
    port = 4000
    path_split = 'partition/'
    path_map = 'map_files/'
    path_reduce = 'reduce_files/'
    path_shuffle = 'shuffle/'
    path_result = 'shuffle/'

    #  Remove temporary files
    data_reader.remove_files_from_multiple_dirs([path_split, path_map, path_reduce, path_shuffle])

    for i in range(len(partition)):
        data_reader.open_file('partition/partition' + str(i) + '.txt', "w")
        data_reader.save_file(partition[i])
        data_reader.close_file()

    master = MasterProcess(host, port, path_split, path_map, path_reduce, path_shuffle, M)  #WRITE operation
    master.start()
    workers = []
    for i in range(M):
        client = WorkerProcess("Worker" + str(i + 1), host, port + i + 1, host, port)
        client.start()
        workers.append(client)

if __name__ == '__main__':
    while True:
        print()
        print("Enter operation to be performed:")
        opt = int(input("1. Write\n2. MapReduce\n3. Read\n4. Exit\n"))
        print()

        if opt == 1:
            print("Input file split into partitions and stored on %d worker nodes\n" % M)
            print()
        elif opt == 2:
            datareader = DataReader()
            datareader.open_file(inputpath, "r")
            flen = len(datareader.file.readlines())
            opt2(inputpath)
            time.sleep(int(flen/3))
        elif opt == 3:
            i = 1
            while os.path.exists("result/result%d.txt" % i):
                i += 1
            i=i-1
            path = "result%d.txt" % i
            path = 'result/'+ path
            with open(path) as reader, open(path, 'r+') as writer:
                for line in reader:
                    if line.strip():
                        writer.write(line)
                writer.truncate()
            print("Output stored in: " + path)
            inputpath = input("Enter file path:\n")
            data_reader = DataReader()
            data_reader.open_file(inputpath, "r")
            file_data = data_reader.file.readlines()
            for line in file_data:
                    print(line)

            data_reader.close_file()
        else:
            print("Exiting!!")
            break




