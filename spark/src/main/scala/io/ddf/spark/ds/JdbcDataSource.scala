package io.ddf.spark.ds

import java.util
import java.util.Properties

import io.ddf.ds.{BaseDataSource, User, UsernamePasswordCredential}
import io.ddf.exception.{DDFException, UnauthenticatedDataSourceException}
import io.ddf.spark.SparkDDFManager
import io.ddf.spark.util.SparkUtils
import io.ddf.{DDF, DDFManager}

class JdbcDataSource(uri: String, manager: DDFManager) extends BaseDataSource(uri, manager) {
  if (!manager.isInstanceOf[SparkDDFManager]) {
    throw new DDFException(s"Loading of $uri is only supported with SparkDDFManager")
  }

  override def loadDDF(user: User, options: util.Map[AnyRef, AnyRef]): DDF = {
    if (!options.containsKey("table")) {
      throw new DDFException("Option table is required to load from JDBC")
    }
    val table = options.get("table").toString
    val connectionUri = getConnectionUri(user, options)
    val hiveContext = manager.asInstanceOf[SparkDDFManager].getHiveContext
    val props = new Properties()
    props.putAll(options)
    val df = hiveContext.read.jdbc(connectionUri, table, props)
    SparkUtils.df2ddf(df, manager)
  }

  def getConnectionUri(user: User, options: util.Map[AnyRef, AnyRef]) = {
    val credential = Option(user.getCredential(uri))
    var url = credential match {
      case Some(credential: UsernamePasswordCredential) =>
        s"$uri?user=${credential.getUsername}&password=${credential.getPassword}"
      case Some(cred) =>
        throw new UnauthenticatedDataSourceException(s"Incompatible credential for JDBC source: $cred")
      case None =>
        s"$uri?1=1"
    }
    if (url.startsWith("jdbc:mysql:")) {
      url = s"$url&zeroDateTimeBehavior=convertToNull"
    }
    url
  }

}
