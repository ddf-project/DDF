package io.ddf.ml;


import io.basic.ddf.BasicDDF;
import io.basic.ddf.content.PersistenceHandler;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.APersistenceHandler;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 */

public class ModelPersistenceTest {

  class DummyModel {
    private double mNumber;
    private String mName;

    public DummyModel(double number, String name) {
      this.mNumber = number;
      this.mName = name;
    }

    public double getNumber() {
      return mNumber;
    }

    public String getName() {
      return mName;
    }
  }

  @Test
  public void testModelSerializationTest() throws DDFException {

    DummyModel dummyModel = new DummyModel(10.0, "dummyModel");
    Model model = new Model(dummyModel);

    String json = model.toJson();

    Model deserializedModel = Model.fromJson(json);

    assertEquals(deserializedModel.getName(), model.getName());
    DummyModel model2 = (DummyModel) deserializedModel.getRawModel();

    assertEquals(model2.getName(), "dummyModel");
    assertEquals(model2.getNumber(), 10.0, 0.001);
  }

  @Test
  public void testModelSerialize2DDF() throws DDFException {
    DummyModel dummyModel = new DummyModel(20, "dummymodel2");
    Model model = new Model(dummyModel);
    DDFManager manager = DDFManager.get("basic");

    DDF ddf = model.serialize2DDF(manager);

    Object obj = ddf.getRepresentationHandler().get(List.class, String.class);
    List<Schema.Column> cols = ddf.getSchema().getColumns();
    List<String> lsString = (List<String>) obj;

    Assert.assertTrue(obj != null);
    Assert.assertTrue(obj instanceof List);
    Assert.assertTrue(ddf != null);

    APersistenceHandler.PersistenceUri uri = ddf.persist();
    PersistenceHandler pHandler = new PersistenceHandler(null);
    DDF ddf2 = (DDF) pHandler.load(uri);
    Model model2 = Model.deserializeFromDDF((BasicDDF) ddf2);

    Assert.assertTrue(ddf2 != null);
    Assert.assertTrue(model2 != null);
    Assert.assertTrue(model2.getRawModel() instanceof DummyModel);
  }
}
