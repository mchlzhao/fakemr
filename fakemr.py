class MapReduce:
    def __init__(self, reader, mapper, reducer):
        self.reader = reader
        self.mapper = mapper
        self.reducer = reducer
    def run(self):
        sortAndShuffler = {}
        for data in self.reader():
            for k, v in self.mapper(value=data):
                if k not in sortAndShuffler:
                    sortAndShuffler[k] = []
                sortAndShuffler[k].append(v)
        ret = []
        for k, v in sortAndShuffler.items():
            for reduced in self.reducer(k, v):
                ret.append(reduced)
        return ret
