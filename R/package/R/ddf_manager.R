#' An S4 class that represents a DistributedDataFrame Manager
#'
#' @param engine the underlying engine, for example: spark or local
#' @exportClass DDFManager
#' @rdname DDFManager
setClass("DDFManager",
         representation(jdm="jobjRef", engine="character"),
         prototype(jdm=NULL, engine="spark")
)

setMethod("initialize",
          signature(.Object="DDFManager"),
          function(.Object, engine) {
            if (is.null(engine))
              engine = "spark"
            .Object@engine = engine
            .Object@jdm = J("io.ddf.DDFManager")$get(engine)
            .Object
          }
)

#' @rdname DDFManager
#' @export
DDFManager <- function(engine="spark") {
  new("DDFManager", engine=engine)
}

#' Execute a HiveQL
#'
#' @param x a DDFManager object
#' @param sql a HiveQL
#' @return a character vector
#' @export
setGeneric("sql",
           function(x, sql, ...) {
             standardGeneric("sql")
           }
)


setMethod("sql",
          signature("DDFManager", "character"),
          function(x, sql) {
            sql <- str_trim(sql)
            jdm <- x@jdm
            java_ret <- jdm$sql2txt(sql)
            sapply(java_ret, function(x) {x$toString()})
          }
)

#' Execute a HiveQL and return a DistributedDataFrame
#'
#' @param x a DDFManager object
#' @param sql the query, only support SELECT ones
#' @return a DistributedDataFrame
#' @export
setGeneric("sql2ddf",
           function(x, sql, ...) {
             standardGeneric("sql2ddf")
           }
)

setMethod("sql2ddf",
          signature("DDFManager", "character"),
          function(x, sql) {
            sql <- str_trim(sql)
            if (!(str_detect(sql, "^[Ss][Ee][Ll][Ee][Cc][Tt].*"))) {
              stop("Only support SELECT queries")
            }
            jdm <- x@jdm
            new("DDF", jdm$sql2ddf(sql))
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
