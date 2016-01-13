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

    def __init__(self, jddf, gateway_client):
        """
        Constructor
        :param jddf: the java DDF object
        :param gateway_client: gateway client
        """
        self._jddf = jddf
        self._gateway_client = gateway_client

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
        return util.parse_ddf_data(res, self.colnames, self.coltypes)

    def project(self, column_names):
        """
        Project on some columns and return a new DistributedDataFrame
        :param column_names:
        """
        return DistributedDataFrame(self._jddf.getViewHandler().project(column_names), self._gateway_client)

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

        return DistributedDataFrame(self._jddf.getViewHandler().getRandomSample(fraction, replacement, seed),
                                    self._gateway_client)

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

    """
    Statistic functions
    """

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

    def var(self, column):
        """
        Compute variance and standard deviation of a DistributedDataFrame's column

        :param column: the column name or index
        :return: a tuple of two elements (variance, standard deviation)
        """
        col_name = util.parse_column_str(self.colnames, column)
        return tuple([float(x) for x in self._jddf.getVectorVariance(col_name)])

    def mean(self, column):
        """
        Calculate Mean value of DDF Column

        :param column: the column name or index
        :return: the mean value
        """
        col_name = util.parse_column_str(self.colnames, column)
        return float(self._jddf.getVectorMean(col_name))

    def drop_na(self, axis='row', inplace=False):
        """
        Drop NA values

        :param axis: the axis by which to drop if NA value exits, ROW represents by Row as default, COLUMN is column.
        :param inplace: whether to treat the ddf inplace, default is FALSE.
        :return: a DDF with no NA values.
        """
        axis = axis.lower()
        if axis not in ['row', 'column']:
            raise ValueError('axis: only "row" or "column" is supported')

        if axis == 'row':
            by = self._gateway_client.jvm.io.ddf.etl.IHandleMissingData.Axis.ROW
        else:
            by = self._gateway_client.jvm.io.ddf.etl.IHandleMissingData.Axis.COLUMN

        if not inplace:
            return DistributedDataFrame(self._jddf.dropNA(by), self._gateway_client)

        self._jddf.setMutable(inplace)
        self._jddf.dropNA(by)
        return self

    def join(self, other, by=None, by_left=None, by_right=None, join_type='inner'):
        """
        Join two DistributedDataFrames together.

        Join, like merge, is designed for the types of problems where you would use a sql join.

        :param other: the other DDF
        :type other: DistributedDataFrame
        :param by: columns used for joining. Default is common columns of `self` and `other`.
        :param by_left: columns used for joining. Default is common columns of `self` and `other`.
        :param by_right: columns used for joining. Default is common columns of `self` and `other`.
        :param join_type: type of join: inner (default), left, right, full or leftsemi
                - inner: only rows with matching keys in both `self` and `other`.
                - left: all rows in `self`, adding matching columns from `other`.
                - right: all rows in `other`, adding matching columns from `self`.
                - full: all rows in `self` with matching columns in `other`,
                        then the rows of `other` that don't match `self`
                - leftsemi: only rows in `self` with matching keys in `other`.
        :return: a DistributedDataFrame
        """
        join_types = ['inner', 'left', 'right', 'full', 'leftsemi']
        join_type = join_type.lower()

        if join_type not in join_types:
            raise ValueError('Invalid join type: {}, valid types are: {}'.format(join_type, ', '.join(join_types)))

        if isinstance(by, basestring):
            by = [by]
        if isinstance(by_left, basestring):
            by_left = [by_left]
        if isinstance(by_right, basestring):
            by_right = [by_right]

        if by is None and by_left is None and by_right is None:
            by = list(set(self.colnames) & set(other.colnames))
            if len(by) == 0:
                raise ValueError('Unable to get the intersection of the column names for joining. Please specify '
                                 'the columns using by, by_x or by_y')

        if join_type == 'inner':
            jjtype = self._gateway_client.jvm.io.ddf.etl.Types.JoinType.INNER
        elif join_type == 'left':
            jjtype = self._gateway_client.jvm.io.ddf.etl.Types.JoinType.LEFT
        elif join_type == 'right':
            jjtype = self._gateway_client.jvm.io.ddf.etl.Types.JoinType.RIGHT
        elif join_type == 'full':
            jjtype = self._gateway_client.jvm.io.ddf.etl.Types.JoinType.FULL
        else:
            jjtype = self._gateway_client.jvm.io.ddf.etl.Types.JoinType.LEFTSEMI

        return DistributedDataFrame(self._jddf.join(other._jddf, jjtype,
                                                    util.to_java_list(by, self._gateway_client),
                                                    util.to_java_list(by_left, self._gateway_client),
                                                    util.to_java_list(by_right, self._gateway_client)),
                                    self._gateway_client)

    def aggregate(self, aggr_columns, by_columns):
        """
        Split the DistributedDataFrame into sub-sets
        by some columns and perform aggregation on some columns within each sub-set

        :param aggr_columns: a list of columns to calculate summary statistics
        :param by_columns: a list of grouping columns
        :return:
        """
        col_names = self.colnames
        col_types = self.coltypes
        if isinstance(aggr_columns, basestring):
            aggr_columns = [aggr_columns]
        if isinstance(by_columns, basestring):
            by_columns = [by_columns]
        if (not isinstance(aggr_columns, list)) or (not isinstance(by_columns, list)) or \
            len(aggr_columns) == 0 or len(by_columns) == 0:
            raise ValueError('Invalid column names')

        if not all([x in col_names for x in by_columns]):
            raise ValueError('Invalid column names in by_columns')

        res = dict(self._jddf.aggregate(','.join(by_columns) + "," + ','.join(aggr_columns)))
        data = []
        for k, v in res.iteritems():
            data.append(k.split('\t') + list(v))
        df = pd.DataFrame(data=data, columns=by_columns+aggr_columns)
        for c in by_columns:
            df[c] = df[c].astype(util.to_python_type(col_types[col_names.index(c)]))
        return df

    def correlation(self, col1, col2):
        """
        Correlation coefficient of a DistributedDataFrame's two numeric columns

        :param col1: a numeric column
        :param col2: a numeric column
        :return: a float
        """
        col_names = self.colnames
        col_types = self.coltypes
        if col1 not in col_names or not util.is_numeric_ddf_type(col_types[col_names.index(col1)]):
            raise ValueError('col1: expect a numeric column name')
        if col2 not in col_names or not util.is_numeric_ddf_type(col_types[col_names.index(col2)]):
            raise ValueError('col2: expect a numeric column name')
        return float(self._jddf.correlation(col1, col2))

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

