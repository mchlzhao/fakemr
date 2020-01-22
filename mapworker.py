class MapWorker:
    def __init__(self, map_func, partitioner):
        self.map_func = map_func
        self.partitioner = partitioner
        self.input = []

    def receive_batch(self, data):
        self.input.extend(data)

    def run(self, output_queue, num_reduce_workers):
        print(len(self.input), ':', self.input)
        print()
        output = []
        for data in self.input:
            output.extend(self.map_func(data))
        reduce_worker_input = [[] for _ in range(num_reduce_workers)]
        for intermediate_pair in output:
            reduce_worker_input[self.partitioner(intermediate_pair[0])].append(intermediate_pair)
        output_queue.put(reduce_worker_input)