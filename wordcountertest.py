import unittest
import wordcounter

class WordCountTest(unittest.TestCase):
    def test(self):
        solver = wordcounter.WordCounter(4, 3)
        solver.solve()
        solver.print_result()
        result = solver.get_result()

        self.assertEqual(len(result), 42)
        self.assertEqual(result['let'], 3)

if __name__ == '__main__':
    unittest.main()