package io.spark.ddf.content

import org.apache.spark.rdd.RDD
import shark.memstore2.TablePartition
import io.ddf.content.Schema.Column

/**
 */
trait CanGetTablePartition {

	def getRDDTablePartition[T](rdd: RDD[T], columns: java.util.List[Column], tableName: String)(implicit ev: CanConvertToTablePartition[T]): RDD[TablePartition] = {
		ev.toTablePartition(rdd, columns, tableName)
	}
}
