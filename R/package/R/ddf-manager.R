#' @include ddf.R
NULL

#' An S4 class that represents a DistributedDataFrame Manager
#' 
#' @param engine the underlying engine, for example: spark or local
#' @exportClass DDFManager
#' @rdname DDFManager
setClass("DDFManager",
         representation(jdm="jobjRef"),
         prototype(jdm=NULL)
)

setMethod("initialize",
          signature(.Object="DDFManager"),
          function(.Object, jdm) {
            .Object@jdm <- jdm
            .Object
          }
)

#' @rdname DDFManager
#' @export
DDFManager <- function(engine=c("spark","flink", "jdbc","sfdc","postgres","aws","redshift","basic")) {
  engine <- match.arg(engine)
  
  if (engine == "spark") {
    new("SparkDDFManager", J("io.ddf.DDFManager")$get(J("io.ddf.DDFManager$EngineType")$SPARK))
  } else if (engine == "flink") {
    new("FlinkDDFManager", J("io.ddf.DDFManager")$get(J("io.ddf.DDFManager$EngineType")$FLINK))
  } else if (engine == "jdbc") {
    new("JDBCDDFManager", J("io.ddf.DDFManager")$get(J("io.ddf.DDFManager$EngineType")$JDBC))
  } else if (engine == "sfdc") {
    new("SFDCDDFManager", J("io.ddf.DDFManager")$get(J("io.ddf.DDFManager$EngineType")$SFDC))
  } else if (engine == "postgres") {
    new("PostgresDDFManager", J("io.ddf.DDFManager")$get(J("io.ddf.DDFManager$EngineType")$POSTGRES))
  } else if (engine == "aws") {
    new("AwsManager", J("io.ddf.DDFManager")$get(J("io.ddf.DDFManager$EngineType")$AWS))
  } else if (engine == "redshift") {
    new("RedshiftDDFManager", J("io.ddf.DDFManager")$get(J("io.ddf.DDFManager$EngineType")$REDSHIFT))
  } else if (engine == "basic") {
    new("BasicDDFManager", J("io.ddf.DDFManager")$get(J("io.ddf.DDFManager$EngineType")$BASIC))
  }
}

#====== Data loading ======================================

#' Load local file
#' 
#' @param x a DDFManager object
#' @param path path to the local file
#' @param sep separator
#' @return a DDF
#' @export
setGeneric("load_file",
           function(x, ...) {
             standardGeneric("load_file")
           }
)

setMethod("load_file",
          signature("DDFManager"),
          function(x, path, sep=" ") {
            jdm <- x@jdm
            java.ret <- jdm$loadFile(path, sep)
            new("DDF", java.ret)
          }
)

#' Load data from JDBC
#' 
#' @param x a DDFManager object
#' @param uri jdbc uri including the database name
#' @param username username
#' @param password password
#' @param table table
#' @return a DDF
#' @export
setGeneric("load_jdbc",
           function(x, uri, username, password, table) {
             standardGeneric("load_jdbc")
           }
)

setMethod("load_jdbc",
          signature("DDFManager"),
          function(x, uri, username, password, table) {
            jdm <- x@jdm
            # Create a JDBCDataSourceDescriptor
            descriptor <- .jnew("io/ddf/datasource/JDBCDataSourceDescriptor", uri, username, password, table)
            java.ret <- jdm$load(descriptor)
            new("DDF", java.ret)
          }
)

#================ Data ETL ===========================

#' Execute a SQL and return an R data.frame
#' 
#' @param x a DDFManager object
#' @param sql the query string
#' @param queryOnDDF whether the query is on ddf, or on the original engine 
#' @return an R data.frame
#' @export
setMethod("sql",
          signature("DDFManager", "character", "logical"),
          function(x, sql, queryOnDDF) {
            sql <- str_trim(sql)
            jdm <- x@jdm
            java.ret <- jdm$sql(sql, queryOnDDF)
            if (!grepl("^create.+$", tolower(sql)) && !grepl("^load.+$", tolower(sql))) {
              parse.sql.result(java.ret)
            }
          }
)

#' Execute a HiveQL and return a DistributedDataFrame
#' 
#' @param x a DDFManager object
#' @param sql the query, only support SELECT ones
#' @return a DistributedDataFrame
#' @export
setMethod("sql2ddf",
          signature("DDFManager", "character", "logical"),
          function(x, sql, queryOnDDF) {
            sql <- str_trim(sql)
            if (!(str_detect(sql, "^[Ss][Ee][Ll][Ee][Cc][Tt].*"))) {
              stop("Only support SELECT queries")
            }
            jdm <- x@jdm
            new("DDF", jdm$sql2ddf(sql, queryOnDDF))
          }
)

#' Shutdown a DDFManager
#' 
#' @param x a DDFManager
#' @export
setGeneric("shutdown",
           function(x) {
             standardGeneric("shutdown")
           }
)

setMethod("shutdown",
          signature("DDFManager"),
          function(x) {
            jdm <- x@jdm
            jdm$shutdown()
            cat('Bye bye\n')
          }
)
