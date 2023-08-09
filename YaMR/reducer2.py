def reduce(key, values):
        # key: a word
        # values: a list of counts
        if sum(values)>1 :
            return key, sum(values)