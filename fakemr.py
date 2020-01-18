import mapworker
import multiprocessing
import reduceworker

def chunked(data, numChunks):
    chunks = [[] for _ in range(numChunks)]
    for i in range(len(data)):
        chunks[i%numChunks].append(data[i])
    return chunks

def parallelize(workers, targetArgs=[]):
    processes = []
    outputQueue = multiprocessing.Queue()
    for worker in workers:
        curProcess = multiprocessing.Process(
            target=worker.run,
            args=[outputQueue]+targetArgs
        )
        curProcess.start()
        processes.append(curProcess)
    for process in processes:
        process.join()
    outputBatches = []
    for _ in range(len(workers)):
        outputBatches.append(outputQueue.get())
    return outputBatches

class MapReduce:
    def __init__(self, reader, mapFunc, reduceFunc, partitioner, numMapWorkers, numReduceWorkers):
        self.reader = reader
        self.mapWorkers = [mapworker.MapWorker(mapFunc, partitioner(numReduceWorkers)) for i in range(numMapWorkers)]
        self.reduceWorkers = [reduceworker.ReduceWorker(reduceFunc) for i in range(numReduceWorkers)]
    
    def run(self):
        inputBatches = chunked(self.reader(), len(self.mapWorkers))
        for i in range(len(self.mapWorkers)):
            self.mapWorkers[i].receiveBatch(inputBatches[i])

        print('Running map workers')
        mapOutputBatches = parallelize(self.mapWorkers, [len(self.reduceWorkers)])
        for batch in mapOutputBatches:
            for i in range(len(self.reduceWorkers)):
                self.reduceWorkers[i].receiveBatch(batch[i])
        
        print('Running reduce workers')
        reduceOutputBatches = parallelize(self.reduceWorkers)
        output = {}
        for batch in reduceOutputBatches:
            output.update(batch)

        return output