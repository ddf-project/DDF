"""
Created on Jun 22, 2014

@author: nhanitvn
"""


class DistributedDataFrame(object):
    """
    A Distributed Data Frame, the basic abstraction in DistributedDataFrame library.
    """

    def __init__(self, jddf):
        """
        Constructor
        """
        self._jddf = jddf

    ###########################################################################

    @property
    def colnames(self):
        """
        List the column names of this DDF

        :return: a list of strings
        """
        return self._jddf.getColumnNames()

    @property
    def rows(self):
        """
        Get number of rows of this DDF

        :return: an int
        """
        return int(self._jddf.getNumRows())

    @property
    def cols(self):
        """
        Get number of columns of this DDF

        :return: an int
        """
        return int(self._jddf.getNumColumns())

    ###########################################################################

    def head(self, n=10):
        """
        Return this DistributedDataFrame's some first rows
        """
        return self._jddf.getViewHandler().head(n)

    def project(self, column_names):
        """
        Project on some columns and return a new DistributedDataFrame
        """
        return DistributedDataFrame(self._jddf.getViewHandler().project(column_names))

    def sample(self, size, replacement=False, seed=123):
        """
        Get a sample of this DistributedDataFrame and return a list of strings

        :param size: number of samples
        :param replacement: sample with or without replacement
        :param seed: random seed
        :return: WRITE ME
        """
        return self._jddf.getViewHandler().getRandomSample(size, replacement, seed)

    def sample2ddf(self, fraction, replacement=False, seed=123):
        """
        Get a sample of this DistributedDataFrame and return a new DistributedDataFrame

        :param fraction: fraction to take sample, has to be in the (0, 1] range
        :param replacement: sample with or without replacement
        :param seed: random seed
        :return: a new DistributedDataFrame
        """
        if fraction <= 0 or fraction > 1:
            raise ValueError('fraction: expected a number in the (0, 1] range')

        return DistributedDataFrame(self._jddf.getViewHandler().getRandomSample(fraction, replacement, seed))

    def summary(self):
        """
        Calculate this DistributedDataFrame's columns'summary numbers
        """
        return self._jddf.getSummary()

    def five_nums(self):
        """
        Calculate Turkey five number for numeric columns
        """
        return self._jddf.getFiveNumSummary()

    def aggregate(self, aggr_columns, by_columns):
        """
        Split the DistributedDataFrame into sub-sets
        by some columns and perform aggregation on some columns within each sub-set
        """
        return self._jddf.aggregate(by_columns + "," + aggr_columns)

    def correlation(self, col1, col2):
        """
        Correlation coefficient of a DistributedDataFrame's two numeric columns

        :param col1: a numeric column
        :param col2: a numeric column
        :return: a float
        """
        return self._jddf.correlation(col1, col2)

    def drop_na(self):
        return DistributedDataFrame(self._jddf.dropNA())
