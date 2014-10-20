package io.spark.ddf.ml;


import io.ddf.exception.DDFException;
import io.ddf.ml.MLClassMethods;
import org.apache.spark.mllib.linalg.Vector$class;
import org.apache.spark.mllib.linalg.Vectors;
/**
 * author: daoduchuan
 */
public class Model extends io.ddf.ml.Model {

  public Model(Object rawModel) {
    super(rawModel);
  }

  @Override
  public Double predict(double[] point) throws DDFException {
    MLClassMethods.PredictMethod predictMethod= new MLClassMethods.PredictMethod(this.getRawModel(), MLClassMethods.DEFAULT_PREDICT_METHOD_NAME,
      new Class<?>[]{Vector$class.class});
    if(predictMethod.getMethod() == null) {
      throw new DDFException(String.format("Cannot locate method specified by %s", MLClassMethods.DEFAULT_PREDICT_METHOD_NAME));

    }
    Object prediction = predictMethod.instanceInvoke(Vectors.dense(point));
    if(prediction instanceof Double) {
      return (Double) prediction;
    } else if (prediction instanceof Integer) {
      return ((Integer) prediction).doubleValue();
    } else {
      throw new DDFException(String.format("Error getting prediction from model %s", this.getRawModel().getClass().getName()));
    }
  }
}
