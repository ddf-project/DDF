'''
Created on Jun 22, 2014

@author: nhanitvn
'''

from ddf.DDF import DDF
from ddf.gateway import start_gateway_server

class DDFManager(object):
    """
    Main entry point for DDF functionality. A SparkDDFManager can be used
    to create DDFs that are implemented for Spark framework.
    """
    _jdm = None
    
    def __init__(self, engineName):
        """
        Constructor
        """
        _gateway = start_gateway_server()
        self._jdm = _gateway.jvm.io.ddf.DDFManager.get(engineName)
        
    def sql(self, command):
        """
        Execute a sql command and return a list of strings
        """
        return self._jdm.sql2txt(command)
    
    
    def sql2ddf(self, command):
        """
        Create a DDF from an sql command.
        """
        return DDF(self._jdm.sql2ddf(command))
    
    def shutdown(self):
        """
        Shut down the DDF Manager
        """
        self._jdm.shutdown()
        print "Bye bye"

if __name__ == '__main__':
    pass