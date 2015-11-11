#' A S4 class that represents a ML model of DistributedDataFrame
#' 
#' @param jmlmodel Java object reference to the backing MLlib model
#' @exportClass MLModel
#' @rdname MLModel
setClass("MLModel",
         representation(jmlmodel="jobjRef"),
         prototype(jmlmodel=NULL)
)

setMethod("initialize",
          signature(.Object="MLModel"),
          function(.Object, jmlmodel) {
            if (is.null(jmlmodel) || !inherits(jmlmodel, "jobjRef") || !(jmlmodel %instanceof% "io.ddf.ml.IModel"))
              stop('MLModel needs a Java object of class "io.ddf.ml.IModel"')
            .Object@jmlmodel = jmlmodel
            .Object
          })

setValidity("MLModel",
            function(object) {
              jmlmodel <- object@jmlmodel
              cls <- jmlmodel$getClass()
              className <- cls$getName()
              if (grep("io.*.ddf.ml.*Model", className) == 1) {
                TRUE
              } else {
                paste("Invalid MLModel class ", className)
              }
            }
)

#' @rdname MLModel
#' @export
MLModel <- function(jmlmodel) {
  new("MLModel", jmlmodel)
}

#' Predict the result of a sample using this ML model
#' 
#' @param m a MLModel
#' @param data the candidate sample data to be predicted,vector is expected
#' @return predict result, class tag for classification, e.g.
#' @export
setGeneric("predict",
           function(m, ...) {
             standardGeneric("predict")
           }
)

setMethod("predict",
          signature("MLModel"),
          function(m, data) {
            res = m@jmlmodel$predict(.jarray(data))
            res
          }
)