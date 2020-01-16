class MapWorker:
    def __init__(self, mapFunc, partitioner):
        self.mapFunc = mapFunc
        self.partitioner = partitioner
        self.input = []
        self.output = []

    def receiveBatch(self, data):
        self.input.extend(data)

    def run(self, outputQueue, numReduceWorkers):
        self.output = map(lambda x: list(x)[0], map(self.mapFunc, self.input))
        reduceWorkerInput = [[] for _ in range(numReduceWorkers)]
        for intermediatePair in self.output:
            reduceWorkerInput[self.partitioner(intermediatePair)].append(intermediatePair)
        outputQueue.put(reduceWorkerInput)