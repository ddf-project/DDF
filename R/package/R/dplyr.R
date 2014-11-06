#' @export
src_DDF <- function(...) {
  ddfm <- DDFManager()
  src_sql("DDF", ddfm)
}

#' @export
src_desc.src_DDF <- function(src) {
  capture.output(print(src$con))
}

#' @export
db_list_tables <- function(con) UseMethod("db_list_tables")

#' @export
db_list_tables.DDFManager <- function(con) {
            ddf::sql(con, sql = "SHOW TABLES")
}

#' @export
db_has_table <- function(con, table) UseMethod("db_has_table")

#' @export
db_has_table.DDFManager <- function(con, table) {
            table %in% db_list_tables(con)
}

#' @export
db_data_type.src_DDF =
  function(src, fields) {
    mapping =
      c(
      integer = "int",
      numeric = "double",
      character = "string",
      factor = "string"
      )
    mapping[sapply(fields, class)]
  }

#' @export
tbl.src_DDF <- function(src, from, ...) {
  tbl_sql("DDF", src = src, from = from, ...)
}


QueryDDF = R6::R6Class(
  "QueryDDF",
  inherit = dplyr:::Query,
  public  =
    list(
      initialize =
        function() {
          private$ddf = sql2ddf(self$con, self$sql)
        },
      fetch =
        function(n = -1L) {
          if(n != -1)
            stop("partial fetch not supported yet")
          head(private$ddf, nrow(private$ddf))
        },
      fetch_paged =
        function(chunk_size, callback) {
          stop("paged fetch not implemented yet")
        },
      nrow = function() nrow(private$ddf)),
  private =
    list(
      ddf = NULL,
      current = 1))

db_explain.src_DDF =
  function(src, sql, mod = c("EXTENDED", "DEPENDENCY")) {
    mod = match.arg(mod)
    ddf::sql(src$con, sql = paste( "EXPLAIN", mod, sql))
  }

sql_escape_ident.src_DDF =
  function(con, x)
    paste0("`", x, "`")

local_tempfile =
  function(...) {
    path = tempfile(...)
    e = new.env()
    e$path = path
    reg.finalizer(e, function(e) unlink(e$fname))
    e
  }


copy_to.src_DDF =
  function(dest, df, name = deparse(substitute(df)), ...) {
    data_tmp = local_tempfile()
    write.table(df, data_tmp$path, row.names = FALSE, col.names = FALSE)
    metastore_tmp = local_tempfile()
    ddf::sql(dm, paste0('set hive.metastore.warehouse.dir=', metastore_tmp$path))
    # not sure why I'd need these two lines
    ddf::sql(
      dest$con,
      paste0(
        "CREATE TABLE ",
        name,
        " (",
        paste(names(df),
              db_data_type(dest, df),
              collapse = ", "),
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '"))
    ddf::sql(dest$con, paste0("LOAD DATA LOCAL INPATH '", data_tmp$path, "' INTO TABLE ", name))
    tbl(sql2ddf(dest$con, paste("select * from ", name)))}

# At this point, all the basic verbs (summarise(), filter(), arrange(), mutate() etc) should also work, but it's hard to test without some data.
#
# copy_to()
#
# Next, implement the methods that power copy_to() work. Once you've implemented these methods, you'll be able copy datasets from R into your database, which will make testing much easier.
#
# db_data_type()
# sql_begin(), sql_commit(), sql_rollback()
# sql_create_table(), sql_insert_into(), sql_drop_table()
# sql_create_index(), sql_analyze()
# If the database doesn't support a function, just return TRUE without doing anything. If you find these methods a very poor match to your backend, you may find it easier to provide a direct copy_to() method.
#
# At this point, you should be able to copy the nycflights13 data packages into your database with (e.g.):
#
# copy_nycflights13(src_mssql(...))
# copy_lahman(src_mssql(...))
# Don't proceed further until this works, and you've verified that the basic single table verbs word.
#
# Query metadata
#
# If you database provides a nice way to access query metadata, implement db_query_fields() and db_query_rows() which return field names and row count for a given query.
#
# Compute, collect and collapse
#
# Next, check that collapse(), compute(), and collect() work.
#
# If collapse() fails, your database has a non-standard way of constructing subqueries. Add a method for sql_subquery().
#
# If compute() fails, your database has a non-standard way of saving queries in temporary tables. Add a method for db_save_query().
#
# Multi table verbs
#
# Next check the multitable verbs:
#
# left_join(), inner_join(): powered by sql_join()
# semi_join(), anti_join(): powered by sql_semi_join()
# union(), intersect(), setdiff(): powered by sql_set_op()
# sql translation
#
# To finish off, you can add custom R -> SQL translation by providing a method for src_translate_env(). This function should return an object created by sql_variant(). See existing methods for examples.
