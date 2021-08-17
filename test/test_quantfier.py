import unittest
import pandas as pd
from app.quantifier import Quantifier
from pandas.testing import assert_frame_equal

class TestModule(unittest.TestCase):
    def test_is_purchased(self):
        testQuantifier = Quantifier("abc")
        #case when event is purchase
        self.assertEqual(testQuantifier.IsPurchased('1', 'pqr'),'pqr')
        #case when event is not purchase
        self.assertEqual(testQuantifier.IsPurchased('2', 'pqr'),None)
    
    def test_calculate_revenue(self):
        testQuantifier = Quantifier("file2")
        param ='Computers;HP Pavillion;1;1000;200|201,Office Supplies;Red Folders;4;4.00;205|206|20'
        #case when product list clean
        self.assertEqual(testQuantifier.CalculateRevenue(param),1016)
        #case when product list is empty
        self.assertEqual(testQuantifier.CalculateRevenue(''),None)
        #case when product list is partially correct
        self.assertEqual(testQuantifier.CalculateRevenue('c1;p1;4;4.00;205|206|20,p2;4'),16)
        #case when product list is None
        self.assertEqual(testQuantifier.CalculateRevenue(None),None)
    
    def test_is_external(self):
        testQuantifier = Quantifier("file2")
        #case when product list clean
        self.assertFalse(testQuantifier.IsExternal(
            'http://www.esshopzilla.com/product/?pid=as2323','http://www.esshopzilla.com/product/?pid=as2323'))
        #case when product list is empty
        self.assertTrue(testQuantifier.IsExternal(
            'http://www.esshopzilla.com/product/?pid=as2323','http://www.google.com'),None)

    def test_transform_data(self):
        testQuantifier = Quantifier("./data/test_data.sql")
        df1 = testQuantifier.ReadFile()
        d1= {'Search Engine Domain': ['bing.com','google.com','bing.com'], 
            'Search Keyword': ['galaxy','ipod','zune'], 
            'Revenue':[1000.0,290.0,250.0]}
        df2 = pd.DataFrame(data=d1)
        df3 = testQuantifier.TransformData(df1)
        assert_frame_equal(df2.reset_index(drop=True), df3.reset_index(drop=True))
        d2 = {'Search Engine Domain': ['google.com','bing.com'], 
            'Search Keyword': ['ipod','zune'], 
            'Revenue':[480.0,250.0]}
        df2 = pd.DataFrame(data=d2)
        self.assertFalse(df2.reset_index(drop=True).equals(df3.reset_index(drop=True)))

if __name__ == '__main__':
    unittest.main()