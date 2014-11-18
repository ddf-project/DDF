/**
 *
 */
package io.ddf.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.ReferenceQueue;
import java.util.LinkedList;

/**
 *
 */
public class PhantomReference extends java.lang.ref.PhantomReference<ISupportPhantomReference> {
  private static Logger LOG = LoggerFactory.getLogger(PhantomReference.class);

  private ISupportPhantomReference mReferent;


  public PhantomReference(ISupportPhantomReference referent, ReferenceQueue<? super ISupportPhantomReference> q) {
    super(referent, q);
    mReferent = referent;
  }

  private void cleanup() {
    if (mReferent != null) mReferent.cleanup();
  }


  private static LinkedList<PhantomReference> phantomReferences = new LinkedList<PhantomReference>();

  private static ReferenceQueue<ISupportPhantomReference> referenceQueue = new ReferenceQueue<ISupportPhantomReference>();


  public static void register(ISupportPhantomReference referent) {
    synchronized (phantomReferences) {
      phantomReferences.add(new PhantomReference(referent, referenceQueue));
    }
    startCleanupThread();
  }


  private static boolean bIsCleanupThreadStarted = false;
  private static Object oCleanupThreadLock = new Object();


  private static void startCleanupThread() {
    if (bIsCleanupThreadStarted) return;

    synchronized (oCleanupThreadLock) {
      if (bIsCleanupThreadStarted) return;
      bIsCleanupThreadStarted = true;
    }

    Thread referenceThread = new Thread() {
      public void run() {
        while (true) {
          try {
            PhantomReference ref = (PhantomReference) referenceQueue.remove();

            LOG.info(String.format("GC removing %s (%s) from memory", ref.getClass().getName(), ref.hashCode()));

            ref.cleanup();
            phantomReferences.remove(ref);
          } catch (Exception ex) {
            // log exception, continue
            LOG.error("Error while cleaning up reference queue", ex);
          }
        }
      }
    };
    referenceThread.setDaemon(true);
    referenceThread.start();
  }
}
