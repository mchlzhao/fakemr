import fakemr
import sys

def reader():
    for line in sys.stdin:
        l = line.lower().split()
        for word in l:
            yield word

def mapper(value, key=None):
    yield value, 1

def reducer(key, values):
    yield key, sum(values)

counter = fakemr.MapReduce(reader, mapper, reducer)
ret = counter.run()
for i in ret:
    print('%s %d' % i)