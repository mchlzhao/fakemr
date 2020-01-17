class MapWorker:
    def __init__(self, mapFunc, partitioner):
        self.mapFunc = mapFunc
        self.partitioner = partitioner
        self.input = []

    def receiveBatch(self, data):
        self.input.extend(data)

    def run(self, outputQueue, numReduceWorkers):
        print(len(self.input), ':', self.input)
        print()
        output = []
        for data in self.input:
            output.extend(self.mapFunc(data))
        reduceWorkerInput = [[] for _ in range(numReduceWorkers)]
        for intermediatePair in output:
            reduceWorkerInput[self.partitioner(intermediatePair[0])].append(intermediatePair)
        outputQueue.put(reduceWorkerInput)