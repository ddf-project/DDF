/**
 * Created by freeman on 7/17/15.
 */


import io.ddf.datasource.{DataSourceURI, JDBCDataSourceDescriptor}
import io.ddf.datasource.JDBCDataSourceDescriptor.JDBCDataSourceCredentials
import io.ddf.jdbc.{JDBCDDF, JDBCDDFManager}
import io.ddf.misc.Config.ConfigConstant


var sourceDescriptor = new JDBCDataSourceDescriptor(
  new DataSourceURI("jdbc:salesforce:User=bhan@adatao.com;Password=KualaLumpur123!@#;SecurityToken=OgBwt0V2NU3fltKAse4sJdmga;"),
  new JDBCDataSourceCredentials("bhan@adatao.com", "KualaLumpur123!@#"),
  null)

val manager = new JDBCDDFManager(sourceDescriptor)

val listTables = manager.showTables()
println(listTables)

val ddf = new JDBCDDF(manager, "adatao/saleforce", "account", "Account");
println(ddf.getColumnNames);

val res = ddf.sql("select * from ddf://adatao/saleforce/account limit 10", "")
println(res)



