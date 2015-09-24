//package io.ddf.example
import java.util.ArrayList
import io.ddf
import io.ddf.DDFManager

object DDFExample {

	def run() = {
		val manager = DDFManager.get(DDFManager.EngineType.SPARK)
		manager.sql("drop TABLE if exists mtcars", "SparkSQL")
		manager.sql("CREATE TABLE mtcars ("
			+ "mpg double,cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
			+ ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '", "SparkSQL")
		manager.sql("LOAD DATA LOCAL INPATH 'resources/test/mtcars' INTO TABLE mtcars", "SparkSQL")

		val ddf = manager.sql2ddf("select * from mtcars", "SparkSQL")
		ddf.getNumRows
		ddf.getNumColumns
		ddf.getColumnNames

		val ddf2 = ddf.VIEWS.project("mpg", "disp", "hp", "drat", "qsec")

		//how to do filter ?????
		//  val ddf3 = ddf2.Views.subset(x$1, x$2)("origin=SFO")
		val ddf3 = ddf2
		ddf3.VIEWS.head(10)

		val col = new ArrayList[String]();
		col.add("mpg")
		val grcol = new ArrayList[String]();
		grcol.add("ampg = avg(mpg)")

		val ddf4 = ddf2.groupBy(col, grcol)

		ddf4.getColumnNames
		ddf4.VIEWS.top(10, "ampg", "asc")

		ddf2.getSummary
		ddf2.getFiveNumSummary

		ddf2.setMutable(true)

		ddf2.dropNA()
		ddf2.getSummary()

		//##########
		//# ML
		//##########
		val kmeans = ddf2.ML.KMeans(3, 5, 1)
		kmeans.predict(Array(24, 22, 1, 3, 5))
	}

}
