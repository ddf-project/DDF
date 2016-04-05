package io.ddf.spark.analytics

import io.ddf.exception.DDFException
import io.ddf.spark.{ATestSuite, SparkDDF}

import scala.collection.JavaConverters._


/**
 * Created by nhanitvn on 3/21/16.
 */
class AggregationHandlerSuite extends ATestSuite {
  createTableAirline()

  test("Proper error message for non-existent columns") {
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL").asInstanceOf[SparkDDF]

    val thrown = intercept[DDFException]{ddf.groupBy(List("Year1").asJava, List("count(*)").asJava)}
    assert(thrown.getMessage === "Non-existent column: Year1")

    val thrown2 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("avg(arrdelay1)").asJava)}
    assert(thrown2.getMessage === "Non-existent column: arrdelay1")

    val thrown3 = intercept[DDFException]{ddf.aggregate("count(*),Year1")}
    assert(thrown3.getMessage === "Non-existent column: Year1")

    val thrown4 = intercept[DDFException]{ddf.aggregate("avg(arrdelay1),Year")}
    assert(thrown4.getMessage === "Non-existent column: arrdelay1")
  }

  test("Proper error message for expressions") {
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL").asInstanceOf[SparkDDF]

    val thrown1 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("arrdelay+").asJava)}
    assert(thrown1.getMessage === "Column or Expression with invalid syntax: 'arrdelay+'")

    val thrown2 = intercept[DDFException]{ddf.aggregate("arrdelay+,Year")}
    assert(thrown2.getMessage === "Column or Expression with invalid syntax: 'arrdelay+'")
  }

  test("Proper error message for undefined function") {
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL").asInstanceOf[SparkDDF]

    val thrown1 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("aaa(arrdelay)").asJava)}
    assert(thrown1.getMessage === "undefined function aaa")
  }

  test("Proper error message for wrong Hive UDF usage") {
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL").asInstanceOf[SparkDDF]

    val thrown1 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("substring('aaaa')").asJava)}
    assert(thrown1.getMessage.contains("No matching method for class org.apache.hadoop.hive.ql.udf.UDFSubstr with (string). Possible choices: _FUNC_(binary, int)  _FUNC_(binary, int, int)  _FUNC_(string, int)  _FUNC_(string, int, int)"))

    val thrown2 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("to_utc_timestamp(1,1,1)").asJava)}
    assert(thrown2.getMessage.contains("The function to_utc_timestamp requires two argument, got 3"))
  }

  test("Proper error message for expressions or columns that contain invalid characters") {
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL").asInstanceOf[SparkDDF]

    val thrown1 = intercept[DDFException]{ddf.groupBy(List("Year@").asJava, List("avg(arrdelay)").asJava)}
    assert(thrown1.getMessage === "Expressions or columns containing invalid character @: Year@")

    val thrown2 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("avg(arrdelay@)").asJava)}
    assert(thrown2.getMessage === "Expressions or columns containing invalid character @: avg(arrdelay@)")

    val thrown3 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("avg(arrdelay @)").asJava)}
    assert(thrown3.getMessage === "Column or Expression with invalid syntax: 'avg(arrdelay @)'")

    val thrown4 = intercept[DDFException]{ddf.groupBy(List("Year @").asJava, List("avg(arrdelay)").asJava)}
    assert(thrown4.getMessage === "Column or Expression with invalid syntax: 'Year @'")

    val thrown5 = intercept[DDFException]{ddf.aggregate("Year@,avg(arrdelay1)")}
    assert(thrown5.getMessage === "Expressions or columns containing invalid character @: Year@")

    val thrown6 = intercept[DDFException]{ddf.aggregate("Year,avg(arrdelay@)")}
    assert(thrown6.getMessage === "Expressions or columns containing invalid character @: AVG(arrdelay@)")
  }

  test("Proper error message for new columns that contain invalid characters") {
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL").asInstanceOf[SparkDDF]

    val thrown1 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("newcol@=avg(arrdelay)").asJava)}
    assert(thrown1.getMessage === "Expressions or columns containing invalid character @: newcol@=avg(arrdelay)")
  }

  test("Proper error message for new columns that exist") {
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL").asInstanceOf[SparkDDF]

    // this should be ok
    val groupped = ddf.groupBy(List("Year").asJava, List("arrdelay=avg(arrdelay)").asJava)
    assert(groupped.getColumnNames.contains("arrdelay"))

    val thrown1 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("Year=avg(arrdelay)").asJava)}
    assert(thrown1.getMessage === "New column name in aggregation cannot be a group by column: Year")

    val thrown2 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("Year = avg(arrdelay)").asJava)}
    assert(thrown2.getMessage === "New column name in aggregation cannot be a group by column: Year")

    val thrown3 = intercept[DDFException]{ddf.groupBy(List("Year").asJava, List("foo=avg(arrdelay)", "foo=sum(arrdelay)").asJava)}
    assert(thrown3.getMessage === "Duplicated column name in aggregations: foo")
  }
}
