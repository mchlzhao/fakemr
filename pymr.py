import multiprocessing
from itertools import groupby

def chunked(data, num_chunks):
    chunks = [[] for _ in range(num_chunks)]
    for i in range(len(data)):
        chunks[i%num_chunks].append(data[i])
    return chunks

def parallelize(workers):
    processes = []
    output_queue = multiprocessing.Queue()
    for worker in workers:
        cur_process = multiprocessing.Process(
            target=worker.run,
            args=[output_queue]
        )
        cur_process.start()
        processes.append(cur_process)
    for process in processes:
        process.join()
    output_batches = []
    for _ in range(len(workers)):
        output_batches.append(output_queue.get())
    return output_batches

class Worker:
    def __init__(self):
        self.input = []

    def receive_input(self, data):
        self.input.extend(data)

    def run(self, output_queue):
        pass

class MapWorker(Worker):
    def __init__(self, map_func, partitioner, num_reducers):
        super().__init__()
        self.map_func = map_func
        self.partitioner = partitioner
        self.reduce_worker_input = [[] for _ in range(num_reducers)]

    def run(self, output_queue):
        print(len(self.input), ':', self.input, '\n')
        output = []
        for data in self.input:
            output.extend(self.map_func(data))
        for intermediate_pair in output:
            self.reduce_worker_input[self.partitioner(intermediate_pair[0])].append(intermediate_pair)
        output_queue.put(self.reduce_worker_input)

class ReduceWorker(Worker):
    def __init__(self, reduce_func):
        super().__init__()
        self.reduce_func = reduce_func

    def run(self, output_queue):
        self.input.sort()
        print(len(self.input), ':', self.input, '\n')
        output = {}
        for k, vs in groupby(self.input, key=lambda x: x[0]):
            output.update(self.reduce_func(k, list(map(lambda x: x[1], vs))))
        output_queue.put(output)

class MasterWorker:
    def __init__(self, reader, map_func, reduce_func, partitioner, num_map_workers, num_reduce_workers):
        self.reader = reader
        self.map_workers = [MapWorker(map_func, partitioner, num_reduce_workers) for i in range(num_map_workers)]
        self.reduce_workers = [ReduceWorker(reduce_func) for i in range(num_reduce_workers)]
    
    def run(self):
        input_batches = chunked(self.reader(), len(self.map_workers))
        for i in range(len(self.map_workers)):
            self.map_workers[i].receive_input(input_batches[i])

        print('Running map workers:')
        map_output_batches = parallelize(self.map_workers)
        for batch in map_output_batches:
            for i in range(len(self.reduce_workers)):
                self.reduce_workers[i].receive_input(batch[i])
        
        print('Running reduce workers:')
        reduce_output_batches = parallelize(self.reduce_workers)
        output = {}
        for batch in reduce_output_batches:
            output.update(batch)

        return output

class Solver:
    def __init__(self, num_mappers=2, num_reducers=2):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.result = None

    def reader(self):
        pass

    def mapper(self):
        pass

    def reducer(self):
        pass

    def partitioner(self, x):
        return hash(str(x)) % self.num_reducers

    def solve(self):
        print('Num map workers = %d\n' % self.num_mappers)
        print('Num reduce workers = %d\n' % self.num_reducers)
        self.result = MasterWorker(
            reader=self.reader,
            map_func=self.mapper,
            reduce_func=self.reducer,
            partitioner=self.partitioner,
            num_map_workers=self.num_mappers,
            num_reduce_workers=self.num_reducers
        ).run()

    def get_result(self):
        return self.result

    def print_result(self):
        print('MapReduce output:')
        for k, v in self.result.items():
            print(k, v)
        print()