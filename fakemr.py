import mapworker
import multiprocessing
import reduceworker

def chunked(data, numChunks):
    chunks = [[] for _ in range(numChunks)]
    for i in range(len(data)):
        chunks[i%numChunks].append(data[i])
    return chunks

class MapReduce:
    def __init__(self, reader, mapFunc, reduceFunc, partitioner=lambda x: 0):
        self.reader = reader
        self.mapFunc = mapFunc
        self.reduceFunc = reduceFunc
        self.partitioner = partitioner
        self.numMapWorkers = 1
        self.numReduceWorkers = 1
        self.output = {}
        self.mapWorkers = None
        self.reduceWorkers = None
    
    def runMapWorkers(self):
        print('Running map workers')
        mapProcesses = []
        outputQueue = multiprocessing.Queue()
        for worker in self.mapWorkers:
            curProcess = multiprocessing.Process(
                target=worker.run,
                args=[outputQueue, self.numReduceWorkers]
            )
            curProcess.start()
            mapProcesses.append(curProcess)
        for process in mapProcesses:
            process.join()
        outputBatches = []
        for _ in range(self.numMapWorkers):
            outputBatches.append(outputQueue.get())
        return outputBatches
    
    def runReduceWorkers(self):
        print('Running reduce workers')
        reduceProcesses = []
        outputQueue = multiprocessing.Queue()
        for worker in self.reduceWorkers:
            curProcess = multiprocessing.Process(target=worker.run, args=[outputQueue])
            curProcess.start()
            reduceProcesses.append(curProcess)
        for process in reduceProcesses:
            process.join()
        outputBatches = []
        for _ in range(self.numReduceWorkers):
            outputBatches.append(outputQueue.get())
        return outputBatches
    
    def run(self):
        self.mapWorkers = [mapworker.MapWorker(self.mapFunc, self.partitioner) for i in range(self.numMapWorkers)]
        self.reduceWorkers = [reduceworker.ReduceWorker(self.reduceFunc) for i in range(self.numReduceWorkers)]

        inputBatches = chunked(self.reader(), self.numMapWorkers)
        for i in range(self.numMapWorkers):
            self.mapWorkers[i].receiveBatch(inputBatches[i])

        mapOutputBatches = self.runMapWorkers()
        for batch in mapOutputBatches:
            for i in range(self.numReduceWorkers):
                self.reduceWorkers[i].receiveBatch(batch[i])
        
        reduceOutputBatches = self.runReduceWorkers()
        for batch in reduceOutputBatches:
            self.output.update(batch)

        return self.output