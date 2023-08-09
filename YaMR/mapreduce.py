import sys
from filehelper import DataReader
from collections import Counter


class MapReduce:

    def __init__(self, task):
        self.task = task

    @staticmethod
    def combine(value):
        # value: document contents
        unique = []
        [unique.append(item) for item in value if item not in unique]
        counter = Counter(value)
        value_dict = counter.items()
        for elem in value_dict:
            unique_ind = unique.index(elem[0])
            temp_list = list(unique[unique_ind])
            temp_list[1] = elem[1]
            unique[unique_ind] = tuple(temp_list)
        return unique


def run(argv):
    from client import mapperfile
    from client import reducerfile
    mapperfile = mapperfile[:-3]
    reducerfile = reducerfile[:-3]
    mapperfile = __import__(mapperfile)
    reducerfile = __import__(reducerfile)
    task = argv[0]
    path_read = argv[1]
    path_save = argv[2]
    path_save_map = "map_files/"

    if task.upper() == "MAP":
        data_reader = DataReader()
        data_reader.open_file(path_read, "r")
        input_data = data_reader.file.read()
        result = mapperfile.map(input_data)
        data_reader.close_file()
        data_reader = DataReader()
        data_reader.open_file(path_save_map + path_read.split("/")[1], "w")
        data_reader.save_map_to_file(result)
        data_reader.close_file()

    elif task.upper() == "REDUCE":
        data_reader = DataReader()
        data_reader.open_file(path_read, "r")
        map_data = data_reader.read_map_from_file()
        data_reader.close_file()
        reduce_result = []
        occurrences = []
        for map_elem in map_data:
            occurrences.append(map_elem[1])
        reduce_result.append(reducerfile.reduce(map_elem[0], occurrences))
        data_writer = DataReader()
        data_writer.open_file(path_save + path_read.split('/')[1], "w")
        for result in reduce_result:
            str_to_save = result[0] + ' ' + str(result[1])
            data_writer.save_file(str_to_save)
        data_writer.close_file()

    elif task.upper() == "COMBINE":
        data_reader = DataReader()
        data_reader.open_file(path_save, "r")
        map_data = data_reader.read_map_from_file()
        result = MapReduce.combine(map_data)
        data_reader.close_file()
        data_reader.save_map_to_file(result)


if __name__ == '__main__':
    run(sys.argv[1:])
