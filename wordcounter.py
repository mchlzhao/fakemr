import fakemr
import sys

def reader():
    with open('anthem.txt', 'r') as f:
        for word in f.read().lower().split():
            yield word

def mapper(value, key=None):
    yield value, 1

def reducer(key, values):
    yield key, sum(values)

def partitioner(data):
    return ord(data[0][0]) % 3

if __name__ == '__main__':
    counter = fakemr.MapReduce(reader=reader, mapper=mapper, reducer=reducer, partitioner=partitioner)
    ret = counter.run()
    for k, v in ret.items():
        print('%s %d' % (k, v))