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

        inputData = self.reader()
        numPerWorker = len(inputData)//self.numMapWorkers
        for i in range(0, len(inputData), numPerWorker):
            mapWorkers[i//numPerWorker].receiveBatch(inputData[i:min(i+numPerWorker, len(inputData))])

        mapProcesses = []
        mapOutputQueue = multiprocessing.Queue()
        print('Running map processes')
        for worker in mapWorkers:
            curProcess = multiprocessing.Process(
                target=worker.run,
                args=[mapOutputQueue, self.numReduceWorkers]
            )
            curProcess.start()
            mapProcesses.append(curProcess)
        for process in mapProcesses:
            process.join()
        
        for _ in range(self.numMapWorkers):
            curBatch = mapOutputQueue.get()
            for i in range(self.numReduceWorkers):
                reduceWorkers[i].receiveBatch(curBatch[i])
        
        reduceProcesses = []
        reduceOutputQueue = multiprocessing.Queue()
        print('Running reduce processes')
        for worker in reduceWorkers:
            curProcess = multiprocessing.Process(target=worker.run, args=[reduceOutputQueue])
            curProcess.start()
            reduceProcesses.append(curProcess)
        for process in reduceProcesses:
            process.join()

        for _ in range(self.numReduceWorkers):
            self.output.update(reduceOutputQueue.get())

        return self.output