import itertools
import mapworker
import reduceworker

class MapReduce:
    def __init__(self, reader, mapFunc, reduceFunc, partitioner=lambda x: 0):
        self.reader = reader
        self.mapFunc = mapFunc
        self.reduceFunc = reduceFunc
        self.partitioner = partitioner
        self.numMapWorkers = 1
        self.numReduceWorkers = 1
        self.output = {}
    def run(self):
        mapWorkers = [mapworker.MapWorker(self.mapFunc, self.partitioner) for i in range(self.numMapWorkers)]
        reduceWorkers = [reduceworker.ReduceWorker(self.reduceFunc) for i in range(self.numReduceWorkers)]
        for data in self.reader():
            mapWorkers[0].receiveInput(data)
        for worker in mapWorkers:
            worker.run(reduceWorkers)
        for worker in reduceWorkers:
            self.output.update(worker.run())
        return self.output