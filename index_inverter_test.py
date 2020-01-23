import unittest
import index_inverter

class InvertIndexTest(unittest.TestCase):
    def test(self):
        solver = index_inverter.IndexInverter(3, 3)
        solver.solve()
        solver.print_result()
        result = solver.get_result()

        self.assertEqual(len(result), 20)
        self.assertEqual(result['Hamilton'], '3')
        self.assertEqual(result['Kvyat'], '1,2')

if __name__ == '__main__':
    unittest.main()