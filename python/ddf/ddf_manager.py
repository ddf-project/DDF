"""
Created on Jun 22, 2014

@author: nhanitvn
"""

from dataframe import DistributedDataFrame
from gateway import start_gateway_server


class DDFManager(object):
    """
    Main entry point for DDF functionality. A SparkDDFManager can be used
    to create DDFs that are implemented for Spark framework.
    """
    _jdm = None

    def __init__(self, engine_name):
        """
        Constructor
        :param engine_name: Name of the DDF engine, e.g. 'spark'
        """
        _gateway = start_gateway_server()
        self._jdm = _gateway.jvm.io.ddf.DDFManager.get(engine_name)

    def sql(self, command):
        """
        Execute a sql command and return a list of strings
        :param command: the sql command to run
        """
        return self._jdm.sql(command)

    def sql2ddf(self, command):
        """
        Create a DistributedDataFrame from an sql command.
        :param command: the sql command to run
        """
        return DistributedDataFrame(self._jdm.sql2ddf(command))

    def shutdown(self):
        """
        Shut down the DDF Manager
        """
        self._jdm.shutdown()
        print('Bye bye')
