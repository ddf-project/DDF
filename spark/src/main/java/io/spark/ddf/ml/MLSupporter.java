package io.spark.ddf.ml;


import io.ddf.DDF;
import io.ddf.content.IHandleRepresentations.IGetResult;
import io.ddf.content.IHandleSchema;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.ml.IModel;
import io.ddf.types.TupleMatrixVector;
import io.ddf.util.Utils.MethodInfo.ParamInfo;
import io.spark.ddf.SparkDDF;
import io.spark.ddf.analytics.CrossValidation;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class MLSupporter extends io.ddf.ml.MLSupporter implements Serializable {

  public MLSupporter(DDF theDDF) {
    super(theDDF);
  }


  /**
   * Override this to return the approriate DDF representation matching that specified in {@link ParamInfo}. The base
   * implementation simply returns the DDF.
   *
   * @param paramInfo
   * @return
   */
  @SuppressWarnings("unchecked")
  @Override
  protected Object convertDDF(ParamInfo paramInfo) throws DDFException {
    mLog.info(">>>> Running ConvertDDF of io.spark.ddf.ml.MLSupporter");
    if (paramInfo.argMatches(RDD.class)) {
      // Yay, our target data format is an RDD!
      RDD<?> rdd = null;

      if (paramInfo.paramMatches(LabeledPoint.class)) {
        rdd = (RDD<LabeledPoint>) this.getDDF().getRepresentationHandler().get(RDD.class, LabeledPoint.class);

      } else if (paramInfo.paramMatches(Vector.class)) {
        rdd = (RDD<Vector>) this.getDDF().getRepresentationHandler().get(RDD.class, Vector.class);
      } else if (paramInfo.paramMatches(double[].class)) {
        rdd = (RDD<double[]>) this.getDDF().getRepresentationHandler().get(RDD.class, double[].class);
      } else if (paramInfo.paramMatches(io.ddf.types.Vector.class)) {
        rdd = (RDD<io.ddf.types.Vector>) this.getDDF().getRepresentationHandler()
            .get(RDD.class, io.ddf.types.Vector.class);
      } else if (paramInfo.paramMatches(TupleMatrixVector.class)) {
        rdd = (RDD<TupleMatrixVector>) this.getDDF().getRepresentationHandler().get(RDD.class, TupleMatrixVector.class);
      } else if (paramInfo.paramMatches(Rating.class)) {
        rdd = (RDD<Rating>) this.getDDF().getRepresentationHandler().get(RDD.class, Rating.class);
      }
      //      else if (paramInfo.paramMatches(TablePartition.class)) {
      //        rdd = (RDD<TablePartition>) this.getDDF().getRepresentationHandler().get(RDD.class, TablePartition.class);
      //      }
      else if (paramInfo.paramMatches(Object.class)) {
        rdd = (RDD<Object[]>) this.getDDF().getRepresentationHandler().get(RDD.class, Object[].class);
      }

      return rdd;
    } else {
      return super.convertDDF(paramInfo);
    }
  }


  @Override
  public DDF applyModel(IModel model) throws DDFException {
    return this.applyModel(model, false, false);
  }

  @Override
  public DDF applyModel(IModel model, boolean hasLabels) throws DDFException {
    return this.applyModel(model, hasLabels, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public DDF applyModel(IModel model, boolean hasLabels, boolean includeFeatures) throws DDFException {
    SparkDDF ddf = (SparkDDF) this.getDDF();
    IGetResult gr = ddf.getJavaRDD(double[].class, Vector.class, LabeledPoint.class, Object[].class);

    // Apply appropriate mapper
    JavaRDD<?> result = null;
    Class<?> resultUnitType = double[].class;

    if (LabeledPoint.class.equals(gr.getTypeSpecs()[0])) {
      mLog.info(">>> applyModel, inputClass= LabeledPoint");
      result = ((JavaRDD<LabeledPoint>) gr.getObject()).mapPartitions(new PredictMapper<LabeledPoint, double[]>(
          LabeledPoint.class, double[].class, model, hasLabels, includeFeatures));

    } else if (double[].class.equals(gr.getTypeSpecs()[0])) {
      mLog.info(">>> applyModel, inputClass= double[]");
      result = ((JavaRDD<double[]>) gr.getObject()).mapPartitions(new PredictMapper<double[], double[]>(double[].class,
          double[].class, model, hasLabels, includeFeatures));

    } else if (Vector.class.equals(gr.getTypeSpecs()[0])) {
      mLog.info(">>> applyModel, inputClass= Vector");
      result = ((JavaRDD<Vector>) gr.getObject()).mapPartitions(new PredictMapper<Vector, double[]>(Vector.class,
          double[].class, model, hasLabels, includeFeatures));

    } else if (Object[].class.equals(gr.getTypeSpecs()[0])) {
      result = ((JavaRDD<Object[]>) gr.getObject()).mapPartitions(new PredictMapper<Object[], Object[]>(Object[].class,
          Object[].class, model, hasLabels, includeFeatures));
      resultUnitType = Object[].class;
    } else {
      throw new DDFException(String.format("Error apply model %s", model.getRawModel().getClass().getName()));
    }


    // Build schema
    List<Schema.Column> outputColumns = new ArrayList<Schema.Column>();

    if (includeFeatures) {
      outputColumns = ddf.getSchema().getColumns();
      // set columns features of result ddf to Double type
      for (Schema.Column col : outputColumns) {
        col.setType(Schema.ColumnType.DOUBLE);
      }
    } else if (!includeFeatures && hasLabels) {
      outputColumns.add(new Schema.Column("ytrue", "double"));
    }
    outputColumns.add(new Schema.Column("yPredict", "double"));

    Schema schema = new Schema(null, outputColumns);

    if (double[].class.equals(resultUnitType)) {
      DDF resultDDF = this.getManager()
          .newDDF(this.getManager(), result.rdd(), new Class<?>[] { RDD.class, double[].class },
              this.getManager().getNamespace(), null, schema);

      IHandleSchema schemaHandler = resultDDF.getSchemaHandler();
      resultDDF.getSchema().setTableName(schemaHandler.newTableName());
      this.getManager().addDDF(resultDDF);

      return resultDDF;
    } else if (Object[].class.equals(resultUnitType)) {
      DDF resultDDF = this.getManager()
          .newDDF(this.getManager(), result.rdd(), new Class<?>[] { RDD.class, Object[].class },
              this.getManager().getNamespace(), null, schema);

      IHandleSchema schemaHandler = resultDDF.getSchemaHandler();
      resultDDF.getSchema().setTableName(schemaHandler.newTableName());
      this.getManager().addDDF(resultDDF);

      return resultDDF;
    } else return null;
  }


  private static class PredictMapper<I, O> implements FlatMapFunction<Iterator<I>, O> {

    private static final long serialVersionUID = 1L;
    private IModel mModel;
    private boolean mHasLabels;
    private boolean mIncludeFeatures;
    private Class<?> mInputType;
    private Class<?> mOutputType;


    public PredictMapper(Class<I> inputType, Class<O> outputType, IModel model, boolean hasLabels,
        boolean includeFeatures) throws DDFException {

      mInputType = inputType;
      mOutputType = outputType;
      mModel = model;
      mHasLabels = hasLabels;
      mIncludeFeatures = includeFeatures;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<O> call(Iterator<I> samples) throws DDFException {
      List<O> results = new ArrayList<O>();


      while (samples.hasNext()) {


        I sample = samples.next();
        O outputRow = null;

        try {
          if (sample instanceof LabeledPoint || sample instanceof double[]) {


            double label = 0;
            double[] features;

            if (sample instanceof LabeledPoint) {
              LabeledPoint s = (LabeledPoint) sample;
              label = s.label();
              features = s.features().toArray();

            } else if(sample instanceof Vector) {
              Vector vector = (Vector) sample;
              if(mHasLabels) {
                label = vector.apply(vector.size() - 1);
                features = Arrays.copyOf(vector.toArray(), vector.size() - 1);
              } else {
                features = vector.toArray();
              }
            } else {
              double[] s = (double[]) sample;
              if (mHasLabels) {
                label = s[s.length - 1];
                features = Arrays.copyOf(s, s.length - 1);
              } else {
                features = s;
              }
            }

            if (double[].class.equals(mOutputType)) {
              if (mHasLabels) {
                outputRow = (O) new double[] { label, (Double) this.mModel.predict(features) };
              } else {
                outputRow = (O) new double[] { (Double) this.mModel.predict(features) };
              }

              if (mIncludeFeatures) {
                outputRow = (O) ArrayUtils.addAll(features, (double[]) outputRow);
              }

            } else if (Object[].class.equals(mOutputType)) {
              if (mHasLabels) {
                outputRow = (O) new Object[] { label, this.mModel.predict(features) };
              } else {
                outputRow = (O) new Object[] { this.mModel.predict(features) };
              }

              if (mIncludeFeatures) {
                Object[] oFeatures = new Object[features.length];
                for (int i = 0; i < features.length; i++) {
                  oFeatures[i] = (Object) features[i];
                }
                outputRow = (O) ArrayUtils.addAll(oFeatures, (Object[]) outputRow);
              }

            } else {
              throw new DDFException(String.format("Unsupported output type %s", mOutputType));
            }


          } else if (sample instanceof Object[]) {
            Object label = null;
            Object[] features;

            Object[] s = (Object[]) sample;
            if (mHasLabels) {
              label = s[s.length - 1];
              features = Arrays.copyOf(s, s.length - 1);
            } else {
              features = s;
            }

            double[] dFeatures = new double[features.length];
            for (int i = 0; i < features.length; i++) {
              dFeatures[i] = (Double) features[i];
            }

            if (mHasLabels) {
              outputRow = (O) new Object[] { label, this.mModel.predict(dFeatures) };
            } else {
              outputRow = (O) new Object[] { this.mModel.predict(dFeatures) };
            }

            if (mIncludeFeatures) {
              outputRow = (O) ArrayUtils.addAll(features, (Object[]) outputRow);
            }

          } else {
            throw new DDFException(String.format("Unsupported input type %s", mInputType));
          }
          results.add(outputRow);

        } catch (Exception e) {
          throw new DDFException(String.format("Error predicting with model %s", this.mModel.getRawModel().getClass()
              .getName()), e);
        }
      }

      return results;
    }
  }


  @Override
  public long[][] getConfusionMatrix(IModel model, double threshold) throws DDFException {
    SparkDDF ddf = (SparkDDF) this.getDDF();
    SparkDDF predictions = (SparkDDF) ddf.ML.applyModel(model, true, false);

    // Now get the underlying RDD to compute
    JavaRDD<double[]> yTrueYPred = (JavaRDD<double[]>) predictions.getJavaRDD(double[].class);
    final double threshold1 = threshold;
    long[] cm = yTrueYPred.map(new Function<double[], long[]>() {
      @Override
      public long[] call(double[] params) {
        byte isPos = toByte(params[0] > threshold1);
        byte predPos = toByte(params[1] > threshold1);

        long[] result = new long[] { 0L, 0L, 0L, 0L };
        result[isPos << 1 | predPos] = 1L;
        return result;
      }
    }).reduce(new Function2<long[], long[], long[]>() {
      @Override
      public long[] call(long[] a, long[] b) {
        return new long[] { a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3] };
      }
    });

    return new long[][] { new long[] { cm[3], cm[2] }, new long[] { cm[1], cm[0] } };
  }

  private byte toByte(boolean exp) {
    if (exp) return 1;
    else return 0;
  }

  public List<List<DDF>> CVKFold(int k, Long seed) throws DDFException {
    return CrossValidation.DDFKFoldSplit(this.getDDF(), k, seed);
  }

  public List<List<DDF>> CVRandom(int k, double trainingSize, Long seed) throws DDFException {
    return CrossValidation.DDFRandomSplit(this.getDDF(), k, trainingSize, seed);
  }
}
