package io.basic.ddf;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BasicDDFTests {

  DDFManager mManager;


  private DDFManager getDDFManager() throws DDFException {
    if (mManager == null) mManager = DDFManager.get("basic");
    return mManager;
  }

  private DDF getTestDDF() throws DDFException {
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(new Object[] { "Last", "Nguyen" });
    list.add(new Object[] { "First", "Christopher" });
    String namespace = null; // use default
    String name = this.getClass().getSimpleName();
    Schema schema = new Schema(name, "name string, value string");
    DDF ddf = ((BasicDDFManager) this.getDDFManager()).newDDF(list, Object[]
            .class, null, namespace, name, schema);
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
}
