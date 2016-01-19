Package: ddf
Maintainer: ADATAO.INC
    <adatao@adatao.com>
Author: ADATAO.INC
Version: 0.1
License: Apache License (== 2.0)
Title: Distributed Data Frame's R interface
Description: This package is the R interface to DDF library. It
    allows R users to work on data reside on a Hadoop cluster
    in a fast, easy and transparent way.
Depends:
    R (>= 3.0),
    rJava,
    stringr,
    methods
Suggests:
    testthat,
    knitr
VignetteBuilder: knitr
RoxygenNote: 5.0.0
Collate:
    'ddf-ml.R'
    'ddf.R'
    'clustering.R'
    'ddf-manager.R'
    'ddf-package.R'
    'regression.R'
    'flink-ddf.R'
    'spark-ddf.R'
    'utils.R'
    'zzz.R'
