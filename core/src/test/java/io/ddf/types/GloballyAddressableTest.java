package io.ddf.types;

import io.ddf.exception.DDFException;
import org.junit.Test;
import junit.framework.Assert;
import io.ddf.DDFManager;
/**
 */
public class GloballyAddressableTest {

  public class MyClass extends AGloballyAddressable {

  }

  @Test
  public void testGloballyAddressable() throws DDFException {
    IGloballyAddressable instance = new MyClass();
    instance.setName("myName");
    Assert.assertEquals(instance.getNamespace(), "adatao");
    Assert.assertEquals(instance.getName(), "myName");
    Assert.assertEquals(instance.getUri(), "MyClass://adatao/myName");
    Assert.assertTrue(instance.getUUID() != null);
    DDFManager manager = DDFManager.get(DDFManager.EngineType.BASIC);
    manager.REGISTRY.register(instance);

    IGloballyAddressable object = manager.REGISTRY.retrieve("adatao", "myName");
    Assert.assertTrue(object instanceof MyClass);
  }
}
