import pymr
import time

def reader():
    with open('testcases/index.txt', 'r') as f:
        return f.read().strip().split('\n')

def mapper(value):
    time.sleep(0.1)
    value = value.split()
    for i in value[1:]:
        yield i, value[0]

def reducer(key, values):
    time.sleep(0.1)
    yield key, ','.join(values)

def getPartitioner(numReducers):
    return lambda x: hash(str(x)) % numReducers

if __name__ == '__main__':
    numMappers = 3
    numReducers = 3
    counter = fakemr.MapReduce(
        reader=reader,
        mapFunc=mapper,
        reduceFunc=reducer,
        partitioner=getPartitioner,
        numMapWorkers=numMappers,
        numReduceWorkers=numReducers
    )
    ret = counter.run()
    for k, v in ret.items():
        print('%s %s' % (k, v))
    print('Num map workers = %d' % numMappers)
    print('Num reduce workers = %d' % numReducers)