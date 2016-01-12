from __future__ import unicode_literals
import unittest

import pandas as pd

import test_base
from ddf import ml


class TestMl(test_base.BaseTest):
    """
    Test ML functions
    """

    def testKmeans(self):
        model = ml.kmeans(self.mtcars, 2, 5, 10)
        self.assertIsInstance(model, ml.KMeansModel)
        self.assertIsInstance(model.centers, pd.DataFrame)
        self.assertEqual(len(model.centers), 2)
        self.assertItemsEqual(model.centers.columns.tolist(), self.mtcars.colnames)

if __name__ == '__main__':
    unittest.main()
