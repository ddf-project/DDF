'''
Created on Jun 22, 2014

@author: nhanitvn
'''

class DDF(object):
    """
    A Distributed Data Frame, the basic abstraction in DDF library.
    """
    def __init__(self, jddf):
        """
        Constructor
        """
        self._jddf = jddf
    
    def getColumnNames(self):
        """
        Return this DDF a list of columns
        """
        return self._jddf.getColumnNames()
        
    def getNumRows(self):
        """
        Return this DDF's number of rows
        """
        return self._jddf.getNumRows()
        
    def getNumColumns(self):
        """
        Return this DDF's number of columns
        """
        return self._jddf.getNumColumns()
    
    def firstNRows(self, numRows):
        """
        Return this DDF's some first rows
        """
        return self._jddf.getViewHandler().head(numRows)

    def project(self, columnNames):
        """
        Project on some columns and return a new DDF
        """
        return DDF(self._jddf.getViewHandler().project(columnNames))
    
    def sample(self, size, withReplacement=False, seed=123):
        """
        Get a sample of this DDF and return a list of strings
        """
        return self._jddf.getViewHandler().getRandomSample(size, withReplacement, seed)

    def sample2DDF(self, percent, withReplacement=False, seed=123):
        """
        Get a sample of this DDF and return a new DDF
        """
        return DDF(self._jddf.getViewHandler().getRandomSample(percent, withReplacement, seed))
    
    def getSummary(self):
        """
        Calculate this DDF's columns'summary numbers
        """
        return self._jddf.getSummary()

    def getFiveNumSummary(self):
        """
        Calculate Turkey five number for numeric columns
        """
        return self._jddf.getFiveNumSummary()
    
    def aggregate(self, aggr_columns, by_columns):
        """
        Split the DDF into sub-sets by some columns and perform aggregation on some columns within each sub-set
        """
        return self._jddf.aggregate(by_columns + "," + aggr_columns)
       
    def correlation(self, columnA, columnB):
        return self._jddf.correlation(columnA, columnB)
    
    def dropNA(self):
        return DDF(self._jddf.dropNA())





if __name__ == '__main__':
    pass
