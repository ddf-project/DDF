#' @include ddf.R ddf-ml.R
NULL

#' ML KMeans
#'
#' Train a Kmeans algorithm
#' @param x a Distributed Data Frame.
#' @param centers number of cluster to be clustered.
#' @param runs number of runs.
#' @param maxIters max times of iterations.
#' @return a KMeanModel object
#' @export
setGeneric("ml.kmeans",
           function(x, ...) {
             standardGeneric("ml.kmeans")
           }
)

setMethod("ml.kmeans",
          signature("DDF"),
          function(x,centers=2,runs=5,maxIters=10) {
            col.names <- colnames(x)
            #numeric.col.indices <- which(sapply(col.names, function(cn) {x@jddf$getColumn(cn)$isNumeric()})==TRUE)
            
            # call java API
            model <- .jrcall(x@jddf$ML,"KMeans",as.integer(centers),as.integer(runs),as.integer(maxIters))
            
            new("KmeansModel", col.names, model)
          }
)

#' A S4 class that represents a KMeans model
#' 
#' @param jmlmodel Java object reference to the backing MLlib model
#' @slot centers a matrix of centers
#' @exportClass KmeansModel
#' @rdname KMeansModel
setClass("KmeansModel",
         slots=list(centers="matrix"),
         contains="MLModel"
)

setMethod("initialize",
          signature(.Object="KmeansModel"),
          function(.Object, col.names, jmlmodel) {
            # Set the cluster centers
            centers <- do.call("rbind", lapply(jmlmodel$getRawModel()$clusterCenters(), function(x) x$toArray()))
            colnames(centers) <- col.names
            rownames(centers) <- paste("Center", rep(1:nrow(centers)))
            .Object@centers <- centers
            # Pass the jmlmodel to the parent
            callNextMethod(.Object, jmlmodel = jmlmodel)
          })

setMethod("summary",
          signature("KmeansModel"),
          function(object,...) {
            cat(paste("Number of clusters: ", nrow(object@centers), "\n\n"))
            
            # print out number of clusters and list of clusters
            cat("Centers:\n")
            print(object@centers)
            
            # TODO: Add total sum of squared error
            # The hard thing is that there is no way to construct command equivalent to the java one
            # kmeansModel.computeCost((RDD<double[]>)ddf.getRepresentationHandler().get(
            # RDD_ARR_DOUBLE().getTypeSpecsString()));
            # Need to have a KMeansModel Java class
          })
