/**
 * Created by freeman on 7/17/15.
 */

import io.ddf.datasource.JDBCDataSourceDescriptor.JDBCDataSourceCredentials
import io.ddf.jdbc.JDBCDDFManager
import io.ddf.datasource._

val sourceDescriptor = new JDBCDataSourceDescriptor(
  new DataSourceURI("jdbc:salesforce:User=bhan@adatao.com;Password=KualaLumpur123!@#;SecurityToken=OgBwt0V2NU3fltKAse4sJdmga;"),
  new JDBCDataSourceCredentials("bhan@adatao.com", "KualaLumpur123!@#"),
  null)

val manager = new JDBCDDFManager(sourceDescriptor)

val listTables = manager.listTables()

println(listTables)
