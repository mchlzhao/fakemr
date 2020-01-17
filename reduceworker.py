import itertools

class ReduceWorker:
    def __init__(self, reduceFunc, customKey=None):
        self.reduceFunc = reduceFunc
        self.sortKey = customKey
        self.input = []
        
    def receiveBatch(self, data):
        self.input.extend(data)

    def run(self, outputQueue):
        self.input.sort(key=self.sortKey)
        print(self.input)
        print()
        output = {}
        for k, vs in itertools.groupby(self.input, key=lambda x: x[0]):
            output.update(self.reduceFunc(k, list(map(lambda x: x[1], vs))))
        outputQueue.put(output)