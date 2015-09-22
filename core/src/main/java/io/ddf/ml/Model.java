package io.ddf.ml;


import com.google.gson.*;
import io.basic.ddf.BasicDDF;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.ml.MLClassMethods.PredictMethod;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;

/**
 */

public class Model implements IModel, Serializable {

  private static final long serialVersionUID = 8076703024981092021L;

  private Object mRawModel;

  private String modelType;

  private String mName;

  private String[] mTrainedColumns;

  private String mClass = this.getClass().getName(); //for serialization

  public Model(Object rawModel) {
    mRawModel = rawModel;

    if (rawModel != null) {
      modelType = mRawModel.getClass().getName();
    } else {
      modelType = null;
    }

    mName = UUID.randomUUID().toString();
  }

  @Override
  public Object getRawModel() {
    return mRawModel;
  }

  @Override
  public void setRawModel(Object rawModel) {
    this.mRawModel = rawModel;
    this.modelType = rawModel.getClass().getName();
  }


  @Override
  public String getName() {
    return mName;
  }

  @Override
  public void setName(String name) {
    mName = name;
  }

  @Override
  public void setTrainedColumns(String[] columns) {
    this.mTrainedColumns = columns;
  }

  @Override
  public String[] getTrainedColumns() {
    return this.mTrainedColumns;
  }

  @Override
  public Double predict(double[] point) throws DDFException {

    PredictMethod predictMethod = new PredictMethod(this.getRawModel(), MLClassMethods.DEFAULT_PREDICT_METHOD_NAME,
        new Class<?>[] { point.getClass() });

    if (predictMethod.getMethod() == null) {
      throw new DDFException(String.format("Cannot locate method specified by %s",
          MLClassMethods.DEFAULT_PREDICT_METHOD_NAME));
    }

    Object prediction = predictMethod.instanceInvoke(point);

    if (prediction instanceof Double) {
      return (Double) prediction;

    } else if (prediction instanceof Integer) {
      return ((Integer) prediction).doubleValue();

    } else {
      throw new DDFException(String.format("Error getting prediction from model %s", this.getRawModel().getClass()
          .getName()));
    }
  }

  @Override
  public String toString() {
    if (modelType.equals("org.apache.spark.mllib.clustering.KMeansModel")) {
      Gson gson = new Gson();
      return String.format("%s \n %s", this.getRawModel().getClass().getName(), gson.toJson(this.getRawModel()));
    } else {
      String modelString = this.getRawModel().toString();
      return modelString;
    }
  }

  @Override
  public String toJson() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

  public static Model fromJson(String json) throws DDFException {
    Gson gson = new GsonBuilder().registerTypeAdapter(Model.class, new ModelDeserializer()).create();

    Model deserializedModel = gson.fromJson(json, Model.class);

    if (deserializedModel == null) {
      throw new DDFException(String.format("Error deserialize json: %s", json));

    } else {
      return deserializedModel;
    }
  }

  @Override
  public DDF serialize2DDF(DDFManager manager) throws DDFException {
    String json = this.toJson();

    Gson gson = new Gson();
    Map<String, Object> keyValueMap = gson.fromJson(json, Map.class);
    JsonObject jsonObject = (new JsonParser().parse(json)).getAsJsonObject();

    Set<String> keyset = keyValueMap.keySet();
    Iterator<String> keys = keyset.iterator();
    List<String> listColumns = new ArrayList<String>();
    List<String> listValues = new ArrayList<String>();

    while (keys.hasNext()) {
      String key = keys.next();
      String value = jsonObject.get(key).toString();
      listColumns.add(String.format(("%s string"), key));
      listValues.add(value);
    }

    // Create schema for DDF
    String columns = StringUtils.join(listColumns, ", ");

    Schema schema = new Schema(this.getName(), columns);

    return new BasicDDF(manager, listValues, String.class, manager
            .getUUID(), manager
            .getNamespace(), this.getName(), schema);
  }

  public static Model deserializeFromDDF(BasicDDF ddf) throws DDFException {
    List<String> data = ddf.getList(String.class);
    List<String> metaData = new ArrayList<String>();
    List<Schema.Column> columns = ddf.getSchema().getColumns();

    for (Schema.Column col : columns) {
      metaData.add(col.getName());
    }
    if (metaData.size() != data.size()) {
      throw new DDFException("");
    }

    List<String> listJson = new ArrayList<String>();

    for (int i = 0; i < metaData.size(); i++) {
      String key = metaData.get(i);
      String value = data.get(i);
      listJson.add(String.format("\"%s\":%s", key, value));
    }

    String json = StringUtils.join(listJson, ",");
    Model model = Model.fromJson(String.format("{%s}", json));

    ddf.getManager().addModel(model);
    return model;
  }

  static class ModelDeserializer implements JsonDeserializer<Model> {
    private Gson _gson = new Gson();

    @Override
    public Model deserialize(JsonElement jElement, Type theType, JsonDeserializationContext context) {

      if (jElement instanceof JsonObject) {
        JsonObject jsonObj = (JsonObject) jElement;
        try {
          String clazz = jsonObj.get("mClass").getAsString();

          if (!clazz.equals(Model.class.getName())) {
            return null;
          }

          Class<?> rawModelClass = Class.forName(jsonObj.get("modelType").getAsString());
          Object rawModel = _gson.fromJson(jsonObj.get("mRawModel"), rawModelClass);
          jsonObj.remove("mRawModel");

          Model deserializedModel = _gson.fromJson(jsonObj, Model.class);
          deserializedModel.setRawModel(rawModel);

          return deserializedModel;
        } catch (Exception e) {
          return null;
        }
      } else {
        return null;
      }
    }
  }
}
