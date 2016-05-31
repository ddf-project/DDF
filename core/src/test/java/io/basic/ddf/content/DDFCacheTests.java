package io.basic.ddf.content;

import io.basic.ddf.BasicDDFManager;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.misc.Config;
import io.ddf.misc.Config.ConfigConstant;
import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class DDFCacheTests {

  private DDFManager getDDFManager() throws DDFException {
    return DDFManager.get(DDFManager.EngineType.BASIC);
  }

  protected DDF getTestDDF(DDFManager manager) throws DDFException {
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(new Object[] { "Last", "Nguyen" });
    list.add(new Object[] { "First", "Christopher" });
    String namespace = "random"; // use default
    Schema schema = new Schema(null, "name string, value string");
    DDF ddf = ((BasicDDFManager) manager).newDDF(list, Object[]
        .class, namespace, null, schema);
    return ddf;
  }

  /**
   * Trigger a GC and make sure it run
   */
  private void runGC() throws Exception {
    WeakReference<Object> weakRef = new WeakReference<Object>(new Object());
    while(weakRef.get() != null) {
      System.gc();
      Thread.sleep(100);
    }
  }

  @Test
  public void cleanupDDFMaximumDDFs() throws Exception {
    ArrayList<WeakReference<DDF>> listDDFs = new ArrayList<WeakReference<DDF>>();
    Config.set(ConfigConstant.SECTION_GLOBAL.toString(), ConfigConstant.MAX_NUMBER_OF_DDFS_IN_CACHE.toString(), "5");
    DDFManager manager = getDDFManager();
    int i = 0;
    while(i < 10) {
      listDDFs.add(new WeakReference<DDF>(getTestDDF(manager)));
      i +=1;
    }
    runGC();

    int removed = 0;
    for(WeakReference<DDF> wDDF: listDDFs) {
      if(wDDF.get() == null) {
        removed += 1;
      }
    }

    Assert.assertTrue(removed == 5);
    Assert.assertTrue(manager.getDDFcache().getCacheStats().evictionCount() == 5);
  }

  @Test
  public void cleanupDDFTimeToIdle() throws Exception {
    ArrayList<WeakReference<DDF>> listDDFs = new ArrayList<WeakReference<DDF>>();
    Config.set(ConfigConstant.SECTION_GLOBAL.toString(), ConfigConstant.DDF_TIME_TO_IDLE_SECONDS.toString(), "1");
    DDFManager manager = getDDFManager();
    int i = 0;
    while(i < 10) {
      listDDFs.add(new WeakReference<DDF>(getTestDDF(manager)));
      i +=1;
    }
    Thread.sleep(5100);

    // eviction is triggered here
    getTestDDF(manager);
    runGC();

    // count number of ddfs that were gced
    int removed = 0;
    for(WeakReference<DDF> wDDF: listDDFs) {
      if(wDDF.get() == null) {
        removed += 1;
      }
    }
    System.out.println("removed = " + removed);
    System.out.println("cacheStats = " + manager.getDDFcache().getCacheStats());
    Assert.assertTrue(removed == 10);
    Assert.assertTrue(manager.getDDFcache().getCacheStats().evictionCount() == 10);
  }
}
