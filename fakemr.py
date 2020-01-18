import mapworker
import multiprocessing
import reduceworker

def chunked(data, numChunks):
    chunks = [[] for _ in range(numChunks)]
    for i in range(len(data)):
        chunks[i%numChunks].append(data[i])
    return chunks

class MapReduce:
    def __init__(self, reader, mapFunc, reduceFunc, partitioner, numMapWorkers, numReduceWorkers):
        self.reader = reader
        self.mapWorkers = [mapworker.MapWorker(mapFunc, partitioner(numReduceWorkers)) for i in range(numMapWorkers)]
        self.reduceWorkers = [reduceworker.ReduceWorker(reduceFunc) for i in range(numReduceWorkers)]
    
    def runMapWorkers(self):
        print('Running map workers')
        mapProcesses = []
        outputQueue = multiprocessing.Queue()
        for worker in self.mapWorkers:
            curProcess = multiprocessing.Process(
                target=worker.run,
                args=[outputQueue, len(self.reduceWorkers)]
            )
            curProcess.start()
            mapProcesses.append(curProcess)
        for process in mapProcesses:
            process.join()
        outputBatches = []
        for _ in range(len(self.mapWorkers)):
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
        for _ in range(len(self.reduceWorkers)):
            outputBatches.append(outputQueue.get())
        return outputBatches
    
    def run(self):
        inputBatches = chunked(self.reader(), len(self.mapWorkers))
        for i in range(len(self.mapWorkers)):
            self.mapWorkers[i].receiveBatch(inputBatches[i])

        mapOutputBatches = self.runMapWorkers()
        for batch in mapOutputBatches:
            for i in range(len(self.reduceWorkers)):
                self.reduceWorkers[i].receiveBatch(batch[i])
        
        output = {}
        reduceOutputBatches = self.runReduceWorkers()
        for batch in reduceOutputBatches:
            output.update(batch)

        return output