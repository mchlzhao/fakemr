import fakemr
import sys
import time

def reader():
    with open('anthem.txt', 'r') as f:
        return f.read().lower().split()

def mapper(value, key=None):
    time.sleep(0.1)
    yield value, 1

def reducer(key, values):
    time.sleep(0.1)
    yield key, sum(values)

def getPartitioner(numReducers):
    return lambda x: ord(x[0][0]) % numReducers

if __name__ == '__main__':
    numMappers = 3
    numReducers = 3
    counter = fakemr.MapReduce(
        reader=reader,
        mapFunc=mapper,
        reduceFunc=reducer,
        partitioner=getPartitioner(numReducers)
    )
    counter.numMapWorkers = numMappers
    counter.numReduceWorkers = numReducers
    ret = counter.run()
    for k, v in ret.items():
        print('%s %d' % (k, v))
    print('Num map workers = %d' % (counter.numMapWorkers))
    print('Num reduce workers = %d' % (counter.numReduceWorkers))