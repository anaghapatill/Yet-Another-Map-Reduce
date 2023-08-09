def map(value):
        # value: document contents
        map_list = []
        for word in value.split():
            map_list.append((word, 1))
        return map_list