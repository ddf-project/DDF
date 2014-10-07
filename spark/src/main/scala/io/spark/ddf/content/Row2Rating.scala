package io.spark.ddf.content

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import shark.api.Row
import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.DDF

class Row2Rating(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val data = representation.getValue.asInstanceOf[RDD[Row]]
    val rddRating = data.map(row ⇒ {

      val user = row.getInt(0)
      val product = row.getInt(1)
      val rating = row.getDouble(2)
      if (user == null || product == null || rating == null) {
        null
      }
      else {
        new Rating(user, product, rating)
      }
    }).filter(row ⇒ row != null)
    new Representation(rddRating, RepresentationHandler.RDD_RATING.getTypeSpecsString)
  }
}
