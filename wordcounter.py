import pymr
import time

def reader():
    with open('testcases/anthem.txt', 'r') as f:
        return f.read().lower().split()

def mapper(value, key=None):
    time.sleep(0.1)
    yield value, 1

def reducer(key, values):
    time.sleep(0.1)
    yield key, sum(values)

def getPartitioner(numReducers):
    return lambda x: hash(str(x)) % numReducers

if __name__ == '__main__':
    numMappers = 4
    numReducers = 3
    counter = pymr.MapReduce(
        reader=reader,
        mapFunc=mapper,
        reduceFunc=reducer,
        partitioner=getPartitioner,
        numMapWorkers=numMappers,
        numReduceWorkers=numReducers
    )
    ret = counter.run()
    for k, v in ret.items():
        print('%s %d' % (k, v))
    print('Num map workers = %d' % numMappers)
    print('Num reduce workers = %d' % numReducers)