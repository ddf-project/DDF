
isInteger <- function(value) {
  value == round(value)
}

# Function for looking up x's values in y and return positions
# Usually for converting column names to column indices
.lookup = function(x, y) {
  if (is.null(x) | is.null(y)) {
    stop("x and y must be not null")
  }
  pos <- match(x, y)
  if (any(is.na(pos))) {
    return((paste0("Can not find these column names: ",paste0(x[which(is.na(pos))],collapse=" "))))
  }  
  return(pos)
}