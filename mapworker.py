class MapWorker:
    def __init__(self, mapFunc, partitioner):
        self.mapFunc = mapFunc
        self.partitioner = partitioner
        self.input = []
        self.output = []
    def receiveInput(self, data):
        self.input.append(data)
    def run(self, reduceWorkers):
        self.output = map(lambda x: list(x)[0], map(self.mapFunc, self.input))
        for i in self.output:
            reduceWorkers[self.partitioner(i)].receiveInput(i)