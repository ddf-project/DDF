"""
Created on Jun 22, 2014

@author: nhanitvn
"""
from __future__ import unicode_literals
import unittest
import pandas as pd

import test_base


class TestDDF(test_base.BaseTest):

    def testDDFBasic(self):
        self.assertEqual(self.airlines.nrow, 31)
        self.assertEqual(self.airlines.ncol, 29)
        self.assertEqual(len(self.airlines), 31)

    def testSummary(self):
        df = self.airlines.summary()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df.columns), self.airlines.ncol)

if __name__ == '__main__':
    unittest.main()
