import unittest
import invertindex

class InvertIndexTest(unittest.TestCase):
    def test(self):
        solver = invertindex.IndexInverter(3, 3)
        solver.solve()
        solver.print_result()
        result = solver.get_result()

        self.assertEqual(len(result), 20)
        self.assertEqual(result['Hamilton'], '3')
        self.assertEqual(result['Kvyat'], '1,2')

if __name__ == '__main__':
    unittest.main()