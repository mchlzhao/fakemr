import unittest
import wordcounter

class WordCountTest(unittest.TestCase):
    def test(self):
        counter = wordcounter.fakemr.MapReduce(wordcounter.reader, wordcounter.mapper, wordcounter.reducer)
        ret = counter.run()
        self.assertEqual(len(ret), 42)
        self.assertEqual(ret['let'], 3)

if __name__ == '__main__':
    unittest.main()