import itertools

class MapReduce:
    def __init__(self, reader, mapper, reducer, partitioner=lambda x: 0):
        self.reader = reader
        self.mapper = mapper
        self.reducer = reducer
        self.partitioner = partitioner
    def run(self):
        s = {}
        for i in map(self.mapper, self.reader()):
            k, v = list(i)[0]
            s.setdefault(k, []).append(v)
        parted = sorted(s.items(), key=self.partitioner)
        print(list(parted))
        for key, group in itertools.groupby(parted, key=self.partitioner):
            print()
            print(key)
            for i in group:
                print(i)
        return dict(map(lambda x: list(x)[0], itertools.starmap(self.reducer, s.items())))
