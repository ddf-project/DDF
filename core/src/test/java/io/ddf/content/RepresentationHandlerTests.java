package io.ddf.content;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * Unit tests for generic DDF.
 */

public class RepresentationHandlerTests {

  private static List<String> list = new ArrayList<String>();


  public class DoubleToObject extends ConvertFunction {

    public DoubleToObject(DDF ddf) {
      super(ddf);
    }

    @Override
    public Representation apply(Representation rep) throws DDFException {
      Double[] obj = (Double[]) rep.getValue();
      List<Object> list = new ArrayList<Object>();
      for (Double elem : obj) {
        list.add((Object) elem);
      }
      Object[] listObj = list.toArray(new Object[list.size()]);
      return new Representation(listObj, Object[].class);
    }
  }


  public class ObjectToInt extends ConvertFunction {

    public ObjectToInt(DDF ddf) {
      super(ddf);
    }

    @Override
    public Representation apply(Representation rep) throws DDFException {
      Object[] obj = (Object[]) rep.getValue();
      List<Integer> list = new ArrayList<Integer>();
      for (Object elem : obj) {
        list.add(((Double) elem).intValue());
      }
      return new Representation(list.toArray(new Integer[list.size()]), Integer[].class);
    }
  }


  public class IntToString extends ConvertFunction {

    public IntToString(DDF ddf) {
      super(ddf);
    }

    @Override
    public Representation apply(Representation rep) throws DDFException {
      Integer[] obj = (Integer[]) rep.getValue();
      List<String> list = new ArrayList<String>();
      for (Integer elem : obj) {
        list.add(elem.toString());
      }
      return new Representation(list.toArray(new String[list.size()]), String[].class);
    }
  }


  public class DummyRepresentationHandler extends RepresentationHandler {
    public DummyRepresentationHandler(DDF ddf) {
      super(ddf);
      Representation rep1 = new Representation(Double[].class);
      Representation rep2 = new Representation(Object[].class);
      Representation rep3 = new Representation(Integer[].class);
      Representation rep4 = new Representation(String[].class);
      this.addConvertFunction(rep1, rep2, new DoubleToObject(this.getDDF()));
      this.addConvertFunction(rep2, rep3, new ObjectToInt(this.getDDF()));
      this.addConvertFunction(rep3, rep4, new IntToString(this.getDDF()));
    }
  }

  @BeforeClass
  public static void setupFixture() {
    list.add("a");
    list.add("b");
    list.add("c");
  }

  @AfterClass
  public static void shutdownFixture() {
  }


  @Test
  public void testRepresentDDF() throws DDFException {
    DDFManager manager = DDFManager.get("basic");
    DDF ddf = manager.newDDF();

    IHandleRepresentations handler = ddf.getRepresentationHandler();

    handler.reset();
    Assert.assertNull("There should not be any existing representations", handler.get(list.get(0).getClass()));

    handler.set(list, List.class, String.class);
    Assert.assertNotNull("There should now be a representation of type <List,String>",
        handler.get(List.class, String.class));

    handler.add(list, List.class, String.class);
    Assert.assertNotNull("There should now be a representation of type <List,String>",
        handler.get(List.class, String.class));

    handler.remove(List.class, String.class);
    Assert.assertNull("There should now be no representation of type <List,String>",
        handler.get(List.class, String.class));
  }

  @Test
  public void testNewRepHandler() throws DDFException {
    DDFManager manager = DDFManager.get("basic");
    DDF ddf = manager.newDDF();

    IHandleRepresentations handler = new DummyRepresentationHandler(ddf);

    Double[] data = new Double[] { 0.0, 2.0, 1.0, 3.0, 4.0, 10.0, 11.0 };
    handler.add(data, Double[].class);
    Object[] obj1 = (Object[]) handler.get(Object[].class);
    Integer[] obj2 = (Integer[]) handler.get(Integer[].class);

    Assert.assertNotNull(obj1);
    Assert.assertNotNull(obj2);
    Assert.assertEquals(handler.getAllRepresentations().size(), 3);
    String[] obj3 = (String[]) handler.get(String[].class);
    Assert.assertNotNull(obj3);
    Assert.assertEquals(handler.getAllRepresentations().size(), 4);
  }
}
