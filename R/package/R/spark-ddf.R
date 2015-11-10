#' @include ddf-manager.R ddf.R
NULL

#============= SparkDDFManager =================================
#' @export
setClass("SparkDDFManager",
         contains = "DDFManager"
)

setMethod("load_file",
          signature("SparkDDFManager"),
          function(x, path, sep=" ") {
            ddf <- callNextMethod(x, path, sep)
            SparkDDF(ddf@jddf)
          }
)

setMethod("load_jdbc",
          signature("DDFManager"),
          function(x, uri, username, password, table) {
            jdm <- x@jdm
            # Create a JDBCDataSourceDescriptor
            descriptor <- .jnew("io/ddf/datasource/JDBCDataSourceDescriptor", uri, username, password, table)
            java.ret <- jdm$load(descriptor)
            new("SparkDDF", java.ret)
          }
)

setMethod("sql2ddf",
          signature("SparkDDFManager", "character"),
          function(x, sql, data.source) {
            ddf <- callNextMethod(x, sql, data.source)
            SparkDDF(ddf@jddf)
          }
)

#============ SparkDDF methods =================================
#' @export
setClass("SparkDDF",
         contains = "DDF"
)

setMethod("initialize",
          signature(.Object="SparkDDF"),
          function(.Object, jddf) {
            callNextMethod(.Object, jddf)
          })

#' @export
SparkDDF <- function(jddf) {
  new("SparkDDF", jddf)
}

setMethod("summary",
          signature("SparkDDF"),
          function(object) {
            cat("This is a SparkDDF\n")
            callNextMethod(object)
          }
)          

setMethod("[", signature(x="SparkDDF"),
          function(x, i,j,...,drop=TRUE) {
            ddf <- callNextMethod(x, i, j,...,drop)
            SparkDDF(ddf@jddf)
          }  
)

setMethod("sample2ddf",
          signature("DDF"),
          function(x, percent, replace=FALSE, seed=123L) {
            ddf <- callNextMethod(x, percent, replace, seed)
            SparkDDF(ddf@jddf)
          }
)

setMethod("sql2ddf",
          signature("SparkDDF"),
          function(x, sql) {
            ddf <- callNextMethod(x, sql)
            SparkDDF(ddf@jddf)
          })

setMethod("merge",
          signature("SparkDDF","SparkDDF"),
          function(x, y, by = intersect(colnames(x), colnames(y)),
                   by.x = by, by.y = by, type=c("inner", "left", "right")) {
            ddf <- callNextMethod(x, y, by, by.x, by.y, type)
            SparkDDF(ddf@jddf)
          })

#============ ML for SparkDDF ==================================
#' Linear Regression using Spark mllib's LinearRegressionSGD
#' @rdname ml.linear.regression
setMethod("ml.linear.regression",
          signature("SparkDDF"),
          function(x, numIterations=10L, stepSize=1, miniBatchFraction=1) {
            col.names <- colnames(x)
            nfeatures <- length(col.names)-1
            col.names <- c("Intercept", col.names[1:nfeatures])
            
            #numeric.col.indices <- which(sapply(col.names, function(cn) {x@jddf$getColumn(cn)$isNumeric()})==TRUE)
            
            # call java API
            model <- .jrcall(x@jddf$ML, "train","linearRegressionWithSGD", 
                             .jarray(list( .jnew("java/lang/Integer",as.integer(numIterations)), 
                                           .jnew("java/lang/Double",stepSize),
                                           .jnew("java/lang/Double", miniBatchFraction))))
            
            weights <- c(model$getRawModel()$intercept(),model$getRawModel()$weights()$toArray())
            names(weights) <- col.names
            
            new("LinearRegressionModel", weights, model)
          }
)

#' Logistic Regression using Spark mllib's Logistic Regression
#' @rdname ml.logistic.regression
setMethod("ml.logistic.regression",
          signature("SparkDDF"),
          function(x, numIterations=10L, stepSize=1, miniBatchFraction) {
            col.names <- colnames(x)
            nfeatures <- length(col.names)-1
            col.names <- c("Intercept", col.names[1:nfeatures])
            #numeric.col.indices <- which(sapply(col.names, function(cn) {x@jddf$getColumn(cn)$isNumeric()})==TRUE)
            
            # call java API
            model <- .jrcall(x@jddf$ML, "train","logisticRegressionWithSGD", 
                             .jarray(list( .jnew("java/lang/Integer",as.integer(numIterations)), .jnew("java/lang/Double",stepSize))))
            
            weights <- c(model$getRawModel()$intercept(),model$getRawModel()$weights()$toArray())
            names(weights) <- col.names
            
            new("LogisticRegressionModel", weights, model)
          }
)

#' Kmeans using Spark mllib's Kmeans
#' @rdname ml.kmeans
setMethod("ml.kmeans",
          signature("SparkDDF"),
          function(x,centers=2,runs=5,maxIters=10) {
            col.names <- colnames(x)
            #numeric.col.indices <- which(sapply(col.names, function(cn) {x@jddf$getColumn(cn)$isNumeric()})==TRUE)
            
            # call java API
            model <- .jrcall(x@jddf$ML,"KMeans",as.integer(centers),as.integer(runs),as.integer(maxIters))
            
            new("KmeansModel", col.names, model)
          }
)