import itertools
import mapworker
import multiprocessing
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
        reduceProcesses = []
        outputQueue = multiprocessing.Queue()
        for worker in reduceWorkers:
            curProcess = multiprocessing.Process(target=worker.run, args=[outputQueue])
            curProcess.start()
            reduceProcesses.append(curProcess)
        for process in reduceProcesses:
            process.join()
        for _ in range(self.numReduceWorkers):
            self.output.update(outputQueue.get())
        return self.output