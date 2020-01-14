import fakemr
import sys
import time

def reader():
    with open('anthem.txt', 'r') as f:
        for word in f.read().lower().split():
            yield word

def mapper(value, key=None):
    yield value, 1

def reducer(key, values):
    time.sleep(0.1)
    yield key, sum(values)

def getPartitioner(numReducers):
    return lambda x: ord(x[0][0]) % numReducers

if __name__ == '__main__':
    numMappers = 1
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
    print('Num reduce workers = %d' % (counter.numReduceWorkers))