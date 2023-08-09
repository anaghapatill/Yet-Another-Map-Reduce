from filehelper import *


def get_file_index_from_key(keys_array, key):
    if key not in keys_array:
        keys_array.append(key)
        return len(keys_array)
    return keys_array.index(key) + 1


def shuffle(path_read, path_save):      #HASH function
    keys_array = []
    files = DataReader.read_all_file_names_from_location(path_read)
    data_reader = DataReader()
    for file in files:
        data_reader.open_file(path_read + file, "r")
        file_data = data_reader.file.readlines()
        for line in file_data:
            key = line.split(' ')[0]
            index = get_file_index_from_key(keys_array, key)
            data_writer = DataReader()
            data_writer.open_file(path_save + 'shuffle' + str(index) + '.txt', "a")
            data_writer.save_file(line)
            data_writer.close_file()
    data_reader.close_file()
    return True
