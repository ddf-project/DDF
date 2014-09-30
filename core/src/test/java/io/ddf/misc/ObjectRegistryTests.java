package io.ddf.misc;


import java.util.Collection;
import junit.framework.Assert;
import org.junit.Test;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import io.ddf.types.IGloballyAddressable;


public class ObjectRegistryTests {

  @Test
  public void testRegister() throws DDFException {
    DDFManager manager = DDFManager.get("basic");
    DDF ddf1 = manager.newDDF();

    Collection<IGloballyAddressable> objs = manager.REGISTRY.getObjects();
    Assert.assertEquals("Initial registry must be empty", 0, objs.size());

    manager.REGISTRY.register(ddf1);
    Assert.assertEquals("Registry must have exactly 1 object", 1, objs.size());

    manager.REGISTRY.register(ddf1);
    Assert.assertEquals("Registry must have exactly 1 object", 1, objs.size());

    DDF ddf2 = (DDF) manager.REGISTRY.retrieve(ddf1.getNamespace(), ddf1.getName());
    Assert.assertEquals("Stored object must equal retrieved object", ddf1, ddf2);

    manager.REGISTRY.unregisterAll();
    objs = manager.REGISTRY.getObjects();
    Assert.assertEquals("Registry must be empty after unregisterAll", 0, objs.size());
  }
}
