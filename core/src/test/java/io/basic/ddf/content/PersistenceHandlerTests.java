/**
 *
 */
package io.basic.ddf.content;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.APersistenceHandler.PersistenceUri;
import io.ddf.exception.DDFException;
import io.ddf.misc.Config.ConfigConstant;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;


/**
 *
 */
public class PersistenceHandlerTests {

  @Test
  public void testPersistenceDir() throws IOException, DDFException {
    DDFManager manager = DDFManager.get(DDFManager.EngineType.BASIC);
    DDF ddf = manager.newDDF();

    List<String> namespaces = ddf.getPersistenceHandler().listNamespaces();
    Assert.assertNotNull(namespaces);

    for (String namespace : namespaces) {
      List<String> ddfs = ddf.getPersistenceHandler().listItems(namespace);
      Assert.assertNotNull(ddfs);
    }
  }

  @Ignore
  public void testPersistDDF() throws Exception {
    DDFManager manager = DDFManager.get(DDFManager.EngineType.BASIC);
    DDF ddf = manager.newDDF();

    PersistenceUri uri = ddf.persist();
    Assert.assertEquals("PersistenceUri must have '" + ConfigConstant.ENGINE_NAME_BASIC + "' for engine/protocol",
        ConfigConstant.ENGINE_NAME_BASIC.toString(), uri.getEngine());
    Assert.assertTrue("Persisted file must exist: " + uri, new File(uri.getPath()).exists());

    ddf.unpersist();
  }
  /*
  @Test
  public void testLoadDDF() throws Exception {
    DDFManager manager = DDFManager.get("basic");
    DDF ddf1 = manager.newDDF();

    PersistenceUri uri = ddf1.persist();

    DDF ddf2 = (DDF) DDFManager.doLoad(uri);
    Assert.assertNotNull(String.format("DDF from doLoad(%s) cannot be null", uri), ddf2);
    Assert.assertEquals("Created and loaded DDF names must be equal", ddf2.getName(), ddf1.getName());

    DDF ddf3 = (DDF) DDFManager.get("basic").load(uri);
    Assert.assertNotNull(String.format("DDF from doLoad(%s) cannot be null", uri), ddf3);
    Assert.assertEquals("Created and loaded DDF names must be equal", ddf3.getName(), ddf1.getName());

    PersistenceUri2 uri2 = new PersistenceUri2(uri);
    DDF ddf4 = (DDF) DDFManager.get("basic").load(uri2.getNamespace(), uri2.getName());
    Assert.assertNotNull(String.format("DDF from doLoad(%s) cannot be null", uri), ddf4);
    Assert.assertEquals("Created and loaded DDF names must be equal", ddf4.getName(), ddf1.getName());

    ddf1.unpersist();
  } */


  /*
  @Test
  public void testPersistModel() throws DDFException {

    Model model = new TestModel(null, null);

    // model.setParameters(new TestParameters());

    PersistenceUri uri = model.persist();
    Assert.assertNotNull("Model persistence URI cannot be null", uri);
    Assert.assertFalse("Model persistence URI cannot be null or empty", Strings.isNullOrEmpty(uri.toString()));

    Model model2 = (Model) DDFManager.doLoad(uri);
    Assert.assertEquals("Models must be the same before and after persistence", model, model2);

    model.unpersist();
  }
  */
}
