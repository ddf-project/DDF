import java.util.Arrays;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import adatao.bigr.thrift.{RCommands,Command,DataFrame,Result};

val transport = new TSocket("localhost", 7911);
transport.open();
val protocol = new TBinaryProtocol(transport);
val client = new RCommands.Client(protocol);

val res = client.execCommand(new Command(null, "connect", null, null));
val sid = res.sid;
println(sid);
