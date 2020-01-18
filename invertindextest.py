import unittest
import invertindex

class InvertIndexTest(unittest.TestCase):
    def test(self):
        numMappers = 3
        numReducers = 3
        counter = invertindex.fakemr.MapReduce(
            reader=invertindex.reader,
            mapFunc=invertindex.mapper,
            reduceFunc=invertindex.reducer,
            partitioner=invertindex.getPartitioner,
            numMapWorkers=numMappers,
            numReduceWorkers=numReducers
        )
        ret = counter.run()
        print(ret)
        self.assertEqual(len(ret), 20)
        self.assertEqual(ret['Hamilton'], '3')
        self.assertEqual(ret['Kvyat'], '1,2')

if __name__ == '__main__':
    unittest.main()