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

def get_partitioner(num_reducers):
    return lambda x: hash(str(x)) % num_reducers

if __name__ == '__main__':
    num_mappers = 3
    num_reducers = 3
    counter = pymr.MapReduce(
        reader=reader,
        map_func=mapper,
        reduce_func=reducer,
        partitioner=get_partitioner,
        num_map_workers=num_mappers,
        num_reduce_workers=num_reducers
    )
    ret = counter.run()
    for k, v in ret.items():
        print('%s %s' % (k, v))
    print('Num map workers = %d' % num_mappers)
    print('Num reduce workers = %d' % num_reducers)