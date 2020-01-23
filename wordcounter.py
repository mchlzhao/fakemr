import pymr
import time

class WordCounter(pymr.MapReduce):
    def reader(self):
        with open('testcases/anthem.txt', 'r') as f:
            return f.read().lower().split()

    def mapper(self, value, key=None):
        time.sleep(0.1)
        yield value, 1

    def reducer(self, key, values):
        time.sleep(0.1)
        yield key, sum(values)

if __name__ == '__main__':
    solver = WordCounter(4, 3)
    solver.solve()
    solver.print_result()