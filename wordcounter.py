import fakemr
import sys

def reader():
    anthemFile = open('anthem.txt', 'r')
    for word in anthemFile.read().lower().split():
        yield word
    anthemFile.close()

def mapper(value, key=None):
    yield value, 1

def reducer(key, values):
    yield key, sum(values)

'''
counter = fakemr.MapReduce(reader, mapper, reducer)
ret = counter.run()
for k, v in ret.items():
    print('%s %d' % (k, v))
'''