namespace java adatao.bigr.thrift.generated

struct Command {
	1: string sid,
	2: string cmdString,
	3: list<string> operants,
	4: string extraOpts
}

struct JsonCommand {
	1: string sid,
	2: string cmdName,
	3: string params //json string
}

struct JsonResult {
	1: string sid,
	2: string result //jsonString
}

struct Result {
	1: string sid,
	2: list<string> sList,
	3: list<i64> iList,
	4: list<double> dList
}

service RCommands {
	Result execCommand(1: Command cmd),
	JsonResult execJsonCommand(1: JsonCommand cmd)
}