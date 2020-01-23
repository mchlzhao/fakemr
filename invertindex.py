import pymr
import time

class IndexInverter(pymr.MapReduce):
    def reader(self):
        with open('testcases/index.txt', 'r') as f:
            return f.read().strip().split('\n')

    def mapper(self, value):
        time.sleep(0.1)
        value = value.split()
        for i in value[1:]:
            yield i, value[0]

    def reducer(self, key, values):
        time.sleep(0.1)
        yield key, ','.join(values)

if __name__ == '__main__':
    solver = IndexInverter(3, 3)
    solver.solve()
    solver.print_result()