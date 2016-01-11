from __future__ import unicode_literals

import os
import sys
import signal

from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from subprocess import Popen, PIPE
from threading import Thread
from conf import DDF_HOME, SCALA_VERSION


_CURRENT_GATEWAY = None


def pre_exec_func():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def compute_classpath(root_path):

    lib_jars = list_jar_files('{}/spark/target/scala-{}/lib'.format(root_path, SCALA_VERSION))
    spark_jars = list_jar_files('{}/spark/target/scala-{}'.format(root_path, SCALA_VERSION))

    return '{}:{}:{}/spark/conf/local'.format(lib_jars, spark_jars, DDF_HOME)


def list_jar_files(path):
    jar_files = [(path + "/" + f) for f in os.listdir(path) if f.endswith('.jar')]
    return ':'.join(map(str, jar_files))


def start_gateway_server():
    classpath = compute_classpath(DDF_HOME)

    java_opts = os.getenv('JAVA_OPTS')
    if java_opts is not None:
        java_opts = java_opts.split()
    else:
        java_opts = []

    # set log options and memory configuration
    if not any([s.startswith('-Dlog4j.configuration') for s in java_opts]):
        java_opts += ['-Dlog4j.configuration=file:{}/core/conf/local/ddf-local-log4j.properties'.format(DDF_HOME)]
    if not any([s.startswith('-Xms') for s in java_opts]):
        java_opts += ['-Xms128m']
    if not any([s.startswith('-Xmx') for s in java_opts]):
        java_opts += ['-Xmx512m']
    if not any([s.startswith('-XX:MaxPermSize') for s in java_opts]):
        java_opts += ['-XX:MaxPermSize=512m']

    command = ['java', '-classpath', classpath] + java_opts + ['py4j.GatewayServer', '--die-on-broken-pipe', '0']

    # launch GatewayServer in a new process
    process = Popen(command, stdout=PIPE, stdin=PIPE, preexec_fn=None)

    # get the port of the GatewayServer
    port = int(process.stdout.readline())

    class JavaOutputThread(Thread):
        def __init__(self, stream):
            Thread.__init__(self)
            self.daemon = True
            self.stream = stream

        def run(self):
            while True:
                line = self.stream.readline()
                sys.stderr.write(line)

    JavaOutputThread(process.stdout).start()

    # connect to the gateway server
    gateway = JavaGateway(GatewayClient(port=port), auto_convert=False)
    java_import(gateway.jvm, 'io.ddf.*')
    java_import(gateway.jvm, 'io.ddf.spark.*')
    return gateway


def current_gateway():
    global _CURRENT_GATEWAY
    if _CURRENT_GATEWAY is None:
        _CURRENT_GATEWAY = start_gateway_server()
    return _CURRENT_GATEWAY


