package io.ddf;


import io.ddf.util.Utils.MethodInfo;
import io.ddf.util.Utils.MethodInfo.ParamInfo;
import junit.framework.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

;

/**
 * Unit tests for generic DDF.
 */
public class DDFTests {

  @Test
  public void testMethodInfo() throws NoSuchMethodException, SecurityException {
    Class<?> thisClass = this.getClass();
    Method testMethod = thisClass.getMethod("testDummy2", String.class, ArrayList.class);
    MethodInfo methodInfo = new MethodInfo(testMethod);
    List<ParamInfo> paramInfos = methodInfo.getParamInfos();

    Assert.assertTrue("First parameter must match String", paramInfos.get(0).argMatches(String.class));
    Assert.assertTrue("Second parameter must match ArrayList<String>",
        paramInfos.get(1).argMatches(ArrayList.class, String.class));
    Assert.assertTrue("Second parameter must match ArrayList<String>", paramInfos.get(1).paramMatches(String.class));

    Assert.assertTrue("First parameter must match String", paramInfos.get(0).argMatches("String"));
    Assert.assertTrue("Second parameter must match ArrayList<String>",
        paramInfos.get(1).argMatches("ArrayList", "String"));
    Assert.assertTrue("Second parameter must match ?<String>", paramInfos.get(1).paramMatches("String"));
  }

  public static void testDummy2(String arg1, ArrayList<String> arg2) {
  }
}
