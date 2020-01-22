import mapworker
import multiprocessing
import reduceworker

def chunked(data, num_chunks):
    chunks = [[] for _ in range(num_chunks)]
    for i in range(len(data)):
        chunks[i%num_chunks].append(data[i])
    return chunks

def parallelize(workers, target_args=[]):
    processes = []
    output_queue = multiprocessing.Queue()
    for worker in workers:
        cur_process = multiprocessing.Process(
            target=worker.run,
            args=[output_queue]+target_args
        )
        cur_process.start()
        processes.append(cur_process)
    for process in processes:
        process.join()
    output_batches = []
    for _ in range(len(workers)):
        output_batches.append(output_queue.get())
    return output_batches

class MapReduce:
    def __init__(self, reader, map_func, reduce_func, partitioner, num_map_workers, num_reduce_workers):
        self.reader = reader
        self.map_workers = [mapworker.MapWorker(map_func, partitioner(num_reduce_workers)) for i in range(num_map_workers)]
        self.reduce_workers = [reduceworker.ReduceWorker(reduce_func) for i in range(num_reduce_workers)]
    
    def run(self):
        input_batches = chunked(self.reader(), len(self.map_workers))
        for i in range(len(self.map_workers)):
            self.map_workers[i].receive_batch(input_batches[i])

        print('Running map workers')
        map_output_batches = parallelize(self.map_workers, [len(self.reduce_workers)])
        for batch in map_output_batches:
            for i in range(len(self.reduce_workers)):
                self.reduce_workers[i].receive_batch(batch[i])
        
        print('Running reduce workers')
        reduce_output_batches = parallelize(self.reduce_workers)
        output = {}
        for batch in reduce_output_batches:
            output.update(batch)

        return output