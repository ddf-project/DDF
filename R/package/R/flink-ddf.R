#' @include ddf-manager.R ddf.R regression.R
NULL

#============= FlinkDDFManager =================================
#' @export
setClass("FlinkDDFManager",
         contains = "DDFManager"
)

setMethod("load_file",
          signature("FlinkDDFManager"),
          function(x, path, sep=" ") {
            ddf <- callNextMethod(x, path, sep)
            FlinkDDF(ddf@jddf)
          }
)

setMethod("load_jdbc",
          signature("FlinkDDFManager"),
          function(x, uri, username, password, table) {
            jdm <- x@jdm
            # Create a JDBCDataSourceDescriptor
            descriptor <- .jnew("io/ddf/datasource/JDBCDataSourceDescriptor", uri, username, password, table)
            java.ret <- jdm$load(descriptor)
            new("FlinkDDF", java.ret)
          }
)

setMethod("sql2ddf",
          signature("FlinkDDFManager", "character"),
          function(x, sql, queryOnDDF) {
            ddf <- callNextMethod(x, sql, queryOnDDF)
            FlinkDDF(ddf@jddf)
          }
)

#============ FlinkDDF methods =================================
#' @export
setClass("FlinkDDF",
         contains = "DDF"
)

setMethod("initialize",
          signature(.Object="FlinkDDF"),
          function(.Object, jddf) {
            callNextMethod(.Object, jddf)
          })

#' @export
FlinkDDF <- function(jddf) {
  new("FlinkDDF", jddf)
}

setMethod("summary",
          signature("FlinkDDF"),
          function(object) {
            cat("This is a FlinkDDF\n")
            callNextMethod(object)
          }
)          

setMethod("[", signature(x="FlinkDDF"),
          function(x, i,j,...,drop=TRUE) {
            ddf <- callNextMethod(x, i, j,...,drop)
            FlinkDDF(ddf@jddf)
          }  
)

setMethod("sample2ddf",
          signature("DDF"),
          function(x, percent, replace=FALSE, seed=123L) {
            ddf <- callNextMethod(x, percent, replace, seed)
            FlinkDDF(ddf@jddf)
          }
)

setMethod("sql2ddf",
          signature("FlinkDDF"),
          function(x, sql) {
            ddf <- callNextMethod(x, sql)
            FlinkDDF(ddf@jddf)
          })

setMethod("merge",
          signature("FlinkDDF","FlinkDDF"),
          function(x, y, by = intersect(colnames(x), colnames(y)),
                   by.x = by, by.y = by, type=c("inner", "left", "right")) {
            ddf <- callNextMethod(x, y, by, by.x, by.y, type)
            FlinkDDF(ddf@jddf)
          })

#============ ML for FlinkDDF ==================================
#' Linear Regression using Flink ML's LinearRegressionSGD
#' @rdname ml.linear.regression
setMethod("ml.linear.regression",
          signature("FlinkDDF"),
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

#' Logistic Regression using Flink ML's Logistic Regression
#' @rdname ml.logistic.regression
setMethod("ml.logistic.regression",
          signature("FlinkDDF"),
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

#' Kmeans using Flink ML's Kmeans
#' @rdname ml.kmeans
setMethod("ml.kmeans",
          signature("FlinkDDF"),
          function(x,centers=2,runs=5,maxIters=10) {
            col.names <- colnames(x)
            #numeric.col.indices <- which(sapply(col.names, function(cn) {x@jddf$getColumn(cn)$isNumeric()})==TRUE)
            
            # call java API
            model <- .jrcall(x@jddf$ML,"KMeans",as.integer(centers),as.integer(runs),as.integer(maxIters))
            
            new("KmeansModel", col.names, model)
          }
)
