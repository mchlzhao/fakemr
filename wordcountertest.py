import unittest
import wordcounter

class WordCountTest(unittest.TestCase):
    def test(self):
        num_mappers = 4
        num_reducers = 3
        counter = wordcounter.pymr.MapReduce(
            reader=wordcounter.reader,
            map_func=wordcounter.mapper,
            reduce_func=wordcounter.reducer,
            partitioner=wordcounter.get_partitioner,
            num_map_workers=num_mappers,
            num_reduce_workers=num_reducers
        )
        ret = counter.run()
        self.assertEqual(len(ret), 42)
        self.assertEqual(ret['let'], 3)

if __name__ == '__main__':
    unittest.main()