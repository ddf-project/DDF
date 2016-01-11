"""
Created on Jun 22, 2014

@author: nhanitvn
"""
from __future__ import unicode_literals

from dataframe import DistributedDataFrame
import gateway
import util


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
        engine_type = gateway.current_gateway().jvm.io.ddf.DDFManager.EngineType.fromString(engine_name)
        self._jdm = gateway.current_gateway().jvm.io.ddf.DDFManager.get(engine_type)

    def list_ddfs(self):
        """
        List all the DDFs

        :return: list of DDF objects
        """
        return [DistributedDataFrame(x) for x in list(self._jdm.listDDFs())]

    def get_ddf_by_name(self, ddf_name):
        """
        Get a DDF object using its name
        :param ddf_name: the name of the DDF object to be retrieved
        :return: a DDF object
        """
        return DistributedDataFrame(self._jdm.getDDFByName(ddf_name))

    def set_ddf_name(self, ddf, name):
        """
        Set a name for the given DDF

        :param ddf: the DDF object
        :param name: name of the DDF
        :return: nothing
        """
        self._jdm.setDDFName(ddf._jddf, name)

    def sql(self, command, data_source='spark'):
        """
        Execute a sql command and return a list of strings
        :param command: the sql command to run
        :param data_source: data source
        """
        command = command.strip()
        res = self._jdm.sql(command, data_source)
        if not (command.lower().startswith('create') or command.lower().startswith('load')):
            return util.parse_sql_result(res)
        return res

    def sql2ddf(self, command, data_source='spark'):
        """
        Create a DistributedDataFrame from an sql command.
        :param command: the sql command to run
        :param data_source: data source
        :return: a DDF
        """
        command = command.strip()
        if not command.lower().startswith('select'):
            raise ValueError('Only SELECT query is supported')

        return DistributedDataFrame(self._jdm.sql2ddf(command, data_source))

    def shutdown(self):
        """
        Shut down the DDF Manager
        """
        self._jdm.shutdown()
