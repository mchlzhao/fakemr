import itertools

class ReduceWorker:
    def __init__(self, reduce_func):
        self.reduce_func = reduce_func
        self.input = []
        
    def receive_batch(self, data):
        self.input.extend(data)

    def run(self, output_queue):
        self.input.sort()
        print(self.input)
        print()
        output = {}
        for k, vs in itertools.groupby(self.input, key=lambda x: x[0]):
            output.update(self.reduce_func(k, list(map(lambda x: x[1], vs))))
        output_queue.put(output)