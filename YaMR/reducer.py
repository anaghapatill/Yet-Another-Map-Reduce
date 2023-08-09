def reduce(key, values):
        # key: a word
        # values: a list of counts
        return key, sum(values)