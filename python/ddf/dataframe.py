"""
Created on Jun 22, 2014

@author: nhanitvn
"""
from __future__ import unicode_literals

import pandas as pd
import numpy as np
import json

import util


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

    def __str__(self):
        name = self.name
        if name is None:
            return 'DistributedDataFrame(rows: {}, columns: {})'.format(self.nrow, self.ncol)
        return 'DistributedDataFrame(name: {}, rows: {}, columns: {})'.format(name, self.nrow, self.ncol)

    def __repr__(self):
        name = self.name
        if name is None:
            return 'DistributedDataFrame(rows: {}, columns: {})'.format(self.nrow, self.ncol)
        return 'DistributedDataFrame(name: {}, rows: {}, columns: {})'.format(name, self.nrow, self.ncol)

    def __len__(self):
        return self.nrow

    ###########################################################################

    @property
    def name(self):
        """
        Get name of this DDF
        :return: a str
        """
        s = self._jddf.getName()
        return s if s is None else str(s)

    @property
    def colnames(self):
        """
        List the column names of this DDF

        :return: a list of strings
        """
        return [str(x) for x in self._jddf.getColumnNames()]

    @property
    def coltypes(self):
        """
        The types of all columns of this DDF

        :return: a list of strings
        """
        return ['{}'.format(self._jddf.getColumn(c).getType()) for c in self.colnames]

    @property
    def rows(self):
        """
        Get number of rows of this DDF

        .. deprecated::
            Use :func:`nrow` instead.

        :return: an int
        """
        return self.nrow

    @property
    def cols(self):
        """
        Get number of columns of this DDF

        .. deprecated::
            Use :func:`ncol` instead.

        :return: an int
        """
        return self.ncol

    @property
    def nrow(self):
        """
        Get number of rows of this DDF

        :return: an int
        """
        return int(self._jddf.getNumRows())

    @property
    def ncol(self):
        """
        Get number of columns of this DDF

        :return: an int
        """
        return int(self._jddf.getNumColumns())

    ###########################################################################

    def head(self, n=10):
        """
        Return this DistributedDataFrame's some first rows
        :param n: number of rows to get
        """
        res = self._jddf.getViewHandler().head(n)
        column_names = self.colnames
        n = len(res)
        data = dict([(c, [None] * n) for c in column_names])
        nulls = ("null", "NULL")
        for i in range(0, n):
            row = str(res[i]).split('\t')
            for j, c in enumerate(column_names):
                value = row[j].replace('\\\\t', '\t')
                if value not in nulls:
                    data[c][i] = value
        return self._convert_type(pd.DataFrame(data=data, columns=column_names), False)

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
        :return: a pandas DataFrame
        """
        res = self._jddf.getViewHandler().getRandomSample(size, replacement, seed)
        df = pd.read_json(json.dumps([list(c) for c in res]))
        df.columns = self.colnames
        return self._convert_type(df, raise_on_error=False)

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
        Return a statistical summary of a DistributedDataFrame's columns
        :return: a pandas DataFrame containing summaries
        """
        data = {}
        ls = list(self._jddf.getSummary())
        for colname, s in zip(self.colnames, ls):
            if s is not None:
                data[colname] = {'mean': float(s.mean()), 'stdev': float(s.stdev()), 'count': int(s.count()),
                                 'cNA': int(s.NACount()), 'min': float(s.min()), 'max': float(s.max())}
            else:
                data[colname] = {'mean': np.nan, 'stdev': np.nan, 'count': np.nan,
                                 'cNA': np.nan, 'min': np.nan, 'max': np.nan}
        return pd.DataFrame(data=data, index=['mean', 'stdev', 'count', 'cNA', 'min', 'max'])

    def five_nums(self):
        """
        Calculate Turkey five number for numeric columns
        :return: a pandas DataFrame in which each column is a vector containing the summary information
        """
        column_names = self.colnames
        data = {}
        five_num_summary = list(self._jddf.getFiveNumSummary())
        labels = ['Min.', '1st Qu.', 'Median', '3rd Qu.', 'Max.']
        for s, col_name in zip(five_num_summary, column_names):
            if self._jddf.getColumn(col_name).isNumeric():
                data[col_name] = dict(zip(labels, [s.getMin(), s.getFirstQuantile(), s.getMedian(),
                                                   s.getThirdQuantile(), s.getMax()]))
        return pd.DataFrame(data=data, index=labels)

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

    ###########################################################################

    def _convert_type(self, df, raise_on_error):
        """
        Utility function to convert the type of a pandas DataFrame
        to make it consistent with this DDF coltypes
        This function also detect and handle JSON data
        :param df: a pandas Data Frame
        :type df: pd.DataFrame
        :param raise_on_error: whether or not to raise Exception if
                there is errors when casting column data types
        :return: type-converted pandas Data Frame
        """
        if (len(self.colnames) != len(df.columns) or
                any([a != b for (a, b) in zip(self.colnames, df.columns.tolist())])):
            raise ValueError('Invalid column names of the data frame')

        return util.convert_column_types(df, self.coltypes, raise_on_error)
