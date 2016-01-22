package io.ddf.spark.ds

import java.sql.{Connection, DriverManager, SQLException, SQLFeatureNotSupportedException}
import java.util.Properties

import io.ddf.ds.{DataSourceCredential, UsernamePasswordCredential}
import io.ddf.exception.{DDFException, InvalidDataSourceCredentialException, UnauthenticatedDataSourceException}
import io.ddf.misc.Config
import io.ddf.spark.SparkDDFManager
import io.ddf.spark.util.SparkUtils
import io.ddf.{DDF, DDFManager}

import scala.collection.JavaConversions._

class JdbcDataSource(uri: String, manager: DDFManager) extends BaseDataSource(uri, manager) {

  val connectionValidateTimeout = Config.getValueOrElseDefault("Spark", "jdbcConnectionValidateTimeout", "10").toInt

  if (!manager.isInstanceOf[SparkDDFManager]) {
    throw new DDFException(s"Loading of $uri is only supported with SparkDDFManager")
  }

  override def loadDDF(options: Map[AnyRef, AnyRef]): DDF = {
    val query = if (options.containsKey("query")) {
      options.get("query").get.toString
    } else if (options.containsKey("table")) {
      val table = options.get("table").get
      s"select * from $table"
    } else {
      throw new DDFException("Required either 'table' or 'query' option to load from JDBC")
    }
    val connectionUri = getConnectionUri(options)
    val hiveContext = manager.asInstanceOf[SparkDDFManager].getHiveContext
    val props = new Properties()
    val optionsAsStrings = options.map { case (key, value) => (s"$key", s"$value") }
    props.putAll(optionsAsStrings)
    val df = hiveContext.read.jdbc(connectionUri, query, props)
    SparkUtils.df2ddf(df, manager)
  }

  def getConnectionUri(options: Map[AnyRef, AnyRef]) = {
    val credential = options.get("credential")
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

  override def validateCredential(credential: DataSourceCredential): Unit = {
    val options = Map[AnyRef, AnyRef]("credential" -> credential)
    val uri = getConnectionUri(options)
    // XXX this try finally block is too Java-ish
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(uri)
      conn.isValid(connectionValidateTimeout)
    } catch {
      // XXX clumsy check and rethrow logic
      case e: SQLFeatureNotSupportedException =>
        // not all driver support isValid method
        // we will accept that the credential is valid if we can get connection
      case e: SQLException =>
        if (JdbcDataSource.isInvalidCredentialError(e)) {
          throw new InvalidDataSourceCredentialException(e.getMessage)
        }
        throw e
      case e => throw e
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }
}

object JdbcDataSource {
  private def isInvalidCredentialError(exception: SQLException): Boolean = {
    val msg = exception.getMessage.toLowerCase
    val accessDenied = msg.contains("access denied")
    val authFailed = msg.contains("authentication failed")
    accessDenied || authFailed
  }
}