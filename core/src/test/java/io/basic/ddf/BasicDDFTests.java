package io.basic.ddf;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class BasicDDFTests {

  DDFManager mManager;


  private DDFManager getDDFManager() throws DDFException {
    if (mManager == null) mManager = DDFManager.get(DDFManager.EngineType.BASIC);
    return mManager;
  }

  private DDF getTestDDF() throws DDFException {
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(new Object[] { "Last", "Nguyen" });
    list.add(new Object[] { "First", "Christopher" });
    String namespace = "random"; // use default
    String name = this.getClass().getSimpleName();
    Schema schema = new Schema(name, "name string, value string");
    DDF ddf = ((BasicDDFManager) this.getDDFManager()).newDDF(list, Object[]
            .class, namespace, name, schema);
    return ddf;
  }

  @Test
  public void testCreateDDF() throws DDFException {
    Assert.assertNotNull("DDFManager cannot be null", this.getDDFManager());

    DDF ddf = this.getDDFManager().newDDF();
    Assert.assertNotNull("DDF cannot be null", ddf);

    Assert.assertNotNull(ddf.getNamespace());
    Assert.assertNotNull(ddf.getUUID());

    DDF ddf2 = this.getTestDDF();
    Assert.assertNotNull("DDF cannot be null", ddf2);
  }

  @Test
  public void testDDFRepresentations() throws DDFException {
    DDF ddf = this.getTestDDF();
    Assert.assertNotNull("DDF cannot be null", ddf);

    Object repr = ddf.getRepresentationHandler().getDefault();
    Assert.assertNotNull("Representation cannot be null", repr);

    @SuppressWarnings("unchecked")
    List<Object[]> list = (List<Object[]>) repr;
    Assert.assertNotNull("List cannot be null", list);

    for (Object[] row : list) {
      Assert.assertNotNull("Row in list cannot be null", row);
    }
  }

  @Test(expected = DDFException.class)
  public void testDDFManagerSetUUID() throws DDFException {
    DDF ddf = this.getTestDDF();
    UUID uuid = ddf.getUUID();
    DDFManager manager =  getDDFManager();

    UUID newUUID = UUID.randomUUID();
    manager.setDDFUUID(ddf, newUUID);
    manager.getDDF(uuid);
  }

  @Test
  public void testDDFMAnager() throws DDFException {
    DDF ddf = this.getTestDDF();
    DDFManager manager = this.getDDFManager();
    manager.setDDFName(ddf, "myddf");

    UUID newUUID = UUID.randomUUID();
    manager.setDDFUUID(ddf, newUUID);

    DDF ddf1 = manager.getDDFByURI(ddf.getUri());
    Assert.assertEquals(ddf1.getUUID(), ddf.getUUID());
    Assert.assertEquals(ddf1.getUUID(), newUUID);
  }

  @Test(expected = DDFException.class)
  public void testRenamingDDF() throws DDFException {
    DDF ddf = this.getTestDDF();
    DDFManager manager = this.getDDFManager();
    manager.setDDFName(ddf, "myddf1");

    String uri1 = ddf.getUri();
    manager.setDDFName(ddf, "myddf2");
    
    manager.getDDFByURI(uri1);
  }
  @Test(expected = DDFException.class)
  public void testDuplicatedColumn() throws DDFException {

    String columns = "row Int, \n\tprice double, \n\t lotsize int, \n\t bedrooms int,\n\tbathrms int, row Int";
    Schema schema = new Schema(null, columns);
    Schema.validateSchema(schema);
  }

  @Test(expected =  DDFException.class)
  public void testDuplicatedColumnInSetColumnNames() throws DDFException {

    String columns = "row Int, \n\tprice double, \n\t lotsize int, \n\t bedrooms int,\n\tbathrms int, row1 Int";
    Schema schema = new Schema(null, columns);
    Schema.validateSchema(schema);
    List<String> columnNames = Arrays.asList("row", "price", "lotsize", "bedrooms", "bathrms", "row");
    schema.setColumnNames(columnNames);
  }

  @Test(expected = DDFException.class)
  public void testGettingInexistedColumn() throws DDFException {
    DDF ddf = this.getTestDDF();
    Schema.Column age = ddf.getSchema().getColumn("age");
  }
}
