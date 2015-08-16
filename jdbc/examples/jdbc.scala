/**
 * Created by freeman on 7/17/15.
 */


import io.ddf.datasource.{DataSourceURI, JDBCDataSourceDescriptor}
import io.ddf.datasource.JDBCDataSourceDescriptor.JDBCDataSourceCredentials
import io.ddf.jdbc.{JDBCDDF, JDBCDDFManager}
import io.ddf.misc.Config.ConfigConstant

var sourceDescriptor = new JDBCDataSourceDescriptor(
  new DataSourceURI("jdbc:mysql://localhost:3306/pinsights"),
  new JDBCDataSourceCredentials("root", ""),
  null)

val manager = new JDBCDDFManager(sourceDescriptor)

val ddf = new JDBCDDF(manager, "adatao/pi", "users", "users");
println(ddf.getColumnNames);

val res = ddf.sql("select * from ddf://adatao/pi/users limit 10", "")
println(res)


