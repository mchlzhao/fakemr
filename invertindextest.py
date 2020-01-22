import unittest
import invertindex

class InvertIndexTest(unittest.TestCase):
    def test(self):
        num_mappers = 3
        num_reducers = 3
        counter = invertindex.pymr.MapReduce(
            reader=invertindex.reader,
            map_func=invertindex.mapper,
            reduce_func=invertindex.reducer,
            partitioner=invertindex.get_partitioner,
            num_map_workers=num_mappers,
            num_reduce_workers=num_reducers
        )
        ret = counter.run()
        print(ret)
        self.assertEqual(len(ret), 20)
        self.assertEqual(ret['Hamilton'], '3')
        self.assertEqual(ret['Kvyat'], '1,2')

if __name__ == '__main__':
    unittest.main()