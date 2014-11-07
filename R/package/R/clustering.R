#' ML model KMeans
#'
#' Return Returns IModel of distributedDataFrame
#' @param x a Distributed Data Frame.
#' @param centers number of cluster to be clustered.
#' @param runs number of runs.
#' @param maxIters max times of iterations.
#' @return a IModel of distributedDataFrame for KMeans clustering
#' @export
setGeneric("ddfKMeans",
           function(x, ...) {
             standardGeneric("ddfKMeans")
           }
)

setMethod("ddfKMeans",
          signature("DDF"),
          function(x,centers=2,runs=5,maxIters=10) {
            col.names <- colnames(x)
            numeric.col.indices <- which(sapply(col.names, function(cn) {x@jddf$getColumn(cn)$isNumeric()})==TRUE)

            # call java API
            model <- .jrcall(x@jddf$ML,"KMeans",as.integer(centers),as.integer(runs),as.integer(maxIters))

            mlmodel = new("MLModel",model)
            mlmodel
          }
)
