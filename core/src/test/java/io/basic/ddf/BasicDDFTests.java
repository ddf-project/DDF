package io.basic.ddf;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
    DDF ddf = ((BasicDDFManager) this.getDDFManager()).newDDF(list, Object[].class, namespace, name, schema);
    return ddf;
  }

  @Test
  public void testCreateDDF() throws DDFException {
    Assert.assertNotNull("DDFManager cannot be null", this.getDDFManager());

    DDF ddf = this.getDDFManager().newDDF();
    Assert.assertNotNull("DDF cannot be null", ddf);

    Assert.assertNotNull(ddf.getNamespace());
    Assert.assertNotNull(ddf.getTableName());
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

}
