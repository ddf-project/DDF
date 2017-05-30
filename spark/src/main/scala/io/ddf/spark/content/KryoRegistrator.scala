package io.ddf.spark.content

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrator}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer, FieldSerializer}
import io.ddf.types.Matrix
import io.ddf.types.Vector
import io.ddf.spark.ml.ROCComputer
import org.jblas.DoubleMatrix
import io.ddf.ml.RocMetric


class KryoRegistrator extends SparkKryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Vector])
    kryo.register(classOf[Matrix])
    kryo.register(classOf[DoubleMatrix])
    kryo.register(classOf[ROCComputer])
    kryo.register(classOf[RocMetric])
    //super.registerClasses(kryo)
  }
}
