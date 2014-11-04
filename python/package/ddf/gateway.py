import os
import sys
import signal

from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from subprocess import Popen, PIPE
from threading import Thread

DDF_HOME = os.environ['DDF_HOME']
SCALA_VERSION = "2.10"

def preexec_func():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def start_gateway_server():
    classPath = compute_classpath(DDF_HOME)
    # launch GatewayServer in a new process
    javaopts = os.getenv('JAVA_OPTS')
    if javaopts is not None:
        javaopts = javaopts.split()
    else:
        javaopts = []
    #command = ["java", "-classpath", classPath] + ["-Dlog4j.configuration=file:"+ DDF_HOME + "/core/conf/local/ddf-local-log4j.properties"] + ["py4j.GatewayServer", "--die-on-broken-pipe", "0"]
    command = ["java", "-classpath", classPath] + javaopts + ["py4j.GatewayServer", "--die-on-broken-pipe", "0"]
    
    proc = Popen(command, stdout = PIPE, stdin = PIPE, preexec_fn = preexec_func)
    # get the port of the GatewayServer
    port = int(proc.stdout.readline())

    class JavaOutputThread(Thread):
        def __init__(self, stream):
            Thread.__init__(self)
            self.daemon = True
            self.stream = stream

        def run(self):
            while True:
                line = self.stream.readline()
                sys.stderr.write(line)
    JavaOutputThread(proc.stdout).start()
    # connect to the gateway server
    gateway = JavaGateway(GatewayClient(port = port), auto_convert = False)
    java_import(gateway.jvm, "io.ddf.*")
    java_import(gateway.jvm, "io.spark.ddf.*")
    return gateway

def compute_classpath(rootPath):
    
    libJars = list_jarfiles(rootPath + "/spark/target/scala-" + SCALA_VERSION + "/lib")
    sparkJars = list_jarfiles(rootPath + "/spark/target/scala-" + SCALA_VERSION)
    py4jJars = list_jarfiles(rootPath + "/python/lib")

    return libJars + ":" + sparkJars + ":" + py4jJars + ":" + DDF_HOME + "/spark/conf/local"

def list_jarfiles(path):
    jarFiles = [(path + "/" + f) for f in os.listdir(path) if f.endswith('.jar')]
    return ':'.join(map(str, jarFiles))

