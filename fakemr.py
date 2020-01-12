import itertools

class MapReduce:
    def __init__(self, reader, mapper, reducer):
        self.reader, self.mapper, self.reducer = reader, mapper, reducer
    def run(self):
        s = {}
        for i in map(self.mapper, self.reader()):
            k, v = list(i)[0]
            s.setdefault(k, []).append(v)
        return dict(map(lambda x: list(x)[0], itertools.starmap(self.reducer, s.items())))
