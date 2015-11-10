#' @include ddf.R ddf-ml.R
NULL

#' ML Linear Regression
#'
#' Train a Linear Regression algorithm
#' @param x a Distributed Data Frame.
#' @param centers number of cluster to be clustered.
#' @param runs number of runs.
#' @param maxIters max times of iterations.
#' @return a KMeanModel object
#' @export
setGeneric("ml.linear.regression",
           function(x, ...) {
             standardGeneric("ml.linear.regression")
           }
)

#' A S4 class that represents a GeneralizedLinearModel model
#' 
#' @param jmlmodel Java object reference to the backing MLlib model
#' @slot centers a matrix of centers
#' @exportClass GeneralizedLinearModel
#' @rdname GeneralizedLinearModel
setClass("GeneralizedLinearModel",
         slots=list(weights="numeric", jmlmodel="jobjRef"),
         contains="MLModel"
)

setMethod("initialize",
          signature(.Object="GeneralizedLinearModel"),
          function(.Object, weights, jmlmodel) {
            if (is.null(jmlmodel) || !inherits(jmlmodel, "jobjRef") || !(jmlmodel %instanceof% "io.ddf.ml.IModel"))
              stop('MLModel needs a Java object of class "io.ddf.ml.IModel"')
            
            # Pass the jmlmodel to the parent
            #callNextMethod(.Object, weights=weights, jmlmodel = jmlmodel)
            .Object@weights <- weights
            .Object@jmlmodel <- jmlmodel
            .Object
          })


#' @exportClass LinearRegressionModel
setClass("LinearRegressionModel",
         contains="GeneralizedLinearModel"
)

setMethod("summary",
          signature("LinearRegressionModel"),
          function(object,...) {
            
            # print out weights
            cat("Weights:\n")
            print(object@weights)
          })


#' @exportClass LogisticRegressionModel
setClass("LogisticRegressionModel",
         contains="GeneralizedLinearModel"
)

#' ML Logistic Regression
#'
#' Train a Logistic Regression algorithm
#' @param x a Distributed Data Frame.
#' @param maxIters max times of iterations.
#' @return a LogisticRegression object
#' @export
setGeneric("ml.logistic.regression",
           function(x, ...) {
             standardGeneric("ml.logistic.regression")
           }
)