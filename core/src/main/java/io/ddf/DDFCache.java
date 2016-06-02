package io.ddf;


import com.google.common.base.Strings;
import com.google.common.cache.*;
import com.google.common.util.concurrent.Striped;
import io.ddf.exception.DDFException;
import io.ddf.misc.ALoggable;
import io.ddf.misc.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 */
public class DDFCache extends ALoggable {

  Striped<Lock> uuidLocks = Striped.lock(Runtime.getRuntime().availableProcessors() * 4);

  /**
   * Keeping DDF without name
   */
  private Cache<UUID, DDF> mDDFCache;

  /**
   * Keeping DDF that has name in a separate cache, so eviction on DDF without name
   * does not affect DDF with name
   */
  private Cache<UUID, DDF> mDDFwithNameCache;

  private DDFManager mDDFManager;

  public DDFCache(DDFManager manager) {

    mDDFManager = manager;

    /**
     * Maximum number of DDFs without name allowed in cache, DDFs will be evicted by an approximate LRU algorithm
     */
    Long maxNumberOfDDFs = Long.valueOf(Config.getGlobalValue(Config.ConfigConstant.MAX_NUMBER_OF_DDFS_IN_CACHE));

    /**
     * Maximum number of DDFs with name allowed in cache, DDFs with name will be evicted by an approximate LRU algorithm
     */
    Long maxNumberOfDDFsWName = Long
        .valueOf(Config.getGlobalValue(Config.ConfigConstant.MAX_NUMBER_OF_DDFS_WITH_NAME_IN_CACHE));

    /**
     * The number of seconds that the DDF will live in cache since it last used(meaning inserted or accessed)
     */
    Long timeToIdle = Long.valueOf(Config.getGlobalValue(Config.ConfigConstant.DDF_TIME_TO_IDLE_SECONDS));

    mLog.info(String.format("Maximum number of ddfs in cache %s", maxNumberOfDDFs));
    mLog.info(String.format("DDF's time to idle = %s seconds", timeToIdle));

    mDDFCache = CacheBuilder.newBuilder().
        maximumSize(maxNumberOfDDFs).expireAfterAccess(timeToIdle, TimeUnit.SECONDS).
        recordStats().removalListener(new DDFRemovalListener()).build();

    // Don't do time based eviction for DDF with name
    mDDFwithNameCache = CacheBuilder.newBuilder().
        maximumSize(maxNumberOfDDFsWName).recordStats().removalListener(new DDFRemovalListener()).build();
  }

  private boolean containsDDF(UUID uuid) {
    return (mDDFCache.asMap().containsKey(uuid) || mDDFwithNameCache.asMap().containsKey(uuid));
  }

  private Map<String, UUID> mUris = new ConcurrentHashMap<String, UUID>();

  public void addDDF(DDF ddf) throws DDFException {
    if (ddf.getUUID() == null) {
      throw new DDFException("uuid is null");
    }
    Lock lock = uuidLocks.get(ddf.getUUID());
    try {
      if (lock.tryLock(120, TimeUnit.SECONDS)) {
        try {
          if (ddf.getName() == null) {
            mDDFCache.put(ddf.getUUID(), ddf);
          } else {
            mDDFwithNameCache.put(ddf.getUUID(), ddf);
          }
        } finally {
          lock.unlock();
        }
      } else {
        throw new DDFException("Timeout getting DDF");
      }
    } catch (InterruptedException e) {
      throw new DDFException(e);
    }
  }

  public CacheStats getCacheStats() {
    return mDDFCache.stats().plus(mDDFwithNameCache.stats());
  }

  public synchronized void removeDDF(DDF ddf) throws DDFException {
    mDDFCache.invalidate(ddf.getUUID());
    mDDFwithNameCache.invalidate(ddf.getUUID());
    if (ddf.getUri() != null) {
      mUris.remove(ddf.getUri());
    }
  }

  public DDF[] listDDFs() {
    List<DDF> ddfs = new ArrayList<DDF>();
    ddfs.addAll(mDDFCache.asMap().values());
    ddfs.addAll(mDDFwithNameCache.asMap().values());
    return ddfs.toArray(new DDF[] {});
  }

  public long size() {
    return mDDFCache.size() + mDDFwithNameCache.size();
  }

  /**
   * Try to get DDF from both mDDFCache and mDDFWithNameCache
   * if DDF is not present in both, do a restore and put it into the respective cache
   *
   * @param uuid
   * @return
   * @throws DDFException
   */
  public DDF getDDF(UUID uuid) throws DDFException {
    Lock lock = uuidLocks.get(uuid);
    try {
      if (lock.tryLock(120, TimeUnit.SECONDS)) {
        try {
          DDF ddf = mDDFCache.getIfPresent(uuid);
          if (ddf == null) {
            ddf = mDDFwithNameCache.getIfPresent(uuid);
            if (ddf == null) {
              ddf = mDDFManager.restoreDDF(uuid);
            }
            if (ddf.getName() != null) {
              mDDFwithNameCache.put(uuid, ddf);
            } else {
              mDDFCache.put(uuid, ddf);
            }
          }
          return ddf;
        } finally {
          lock.unlock();
        }
      } else {
        throw new DDFException("Timeout getting DDF");
      }
    } catch (Exception e) {
      throw new DDFException(String.format("DDF with uuid %s does not exist", uuid), e);
    }
  }

  public DDF getDDFByName(String name) throws DDFException {
    for (DDF ddf : this.listDDFs()) {
      if (!Strings.isNullOrEmpty(ddf.getName()) && ddf.getName().equals(name)) {
        return ddf;
      }
    }
    throw new DDFException(String.format("Cannot find ddf with name %s", name));
  }

  public void setDDFName(DDF ddf, String name) throws DDFException {
    Lock lock = uuidLocks.get(ddf.getUUID());
    try {
      if(lock.tryLock(120, TimeUnit.SECONDS)) {
        try {
          if (!Strings.isNullOrEmpty(name)) {
            if (!Strings.isNullOrEmpty(ddf.getName())) {
              this.mUris.remove(ddf.getUri());
            }
            ddf.setName(name);
            mDDFCache.invalidate(ddf.getUUID());
            mDDFwithNameCache.put(ddf.getUUID(), ddf);
            this.mUris.put(ddf.getUri(), ddf.getUUID());
          } else {
            throw new DDFException(String.format("DDF's name cannot be null or empty"));
          }
        } finally {
          lock.unlock();
        }
      } else {
        throw new DDFException("Timeout setting DDF name");
      }
    } catch (Exception e) {
      throw new DDFException(e);
    }
  }

  public synchronized void setDDFUUID(DDF ddf, UUID uuid) throws DDFException {
    if (this.containsDDF(uuid)) {
      throw new DDFException(String.format("DDF with uuid %s already exists", uuid));
    } else {
      UUID prevUUID = ddf.getUUID();
      ddf.setUUID(uuid);
      if (prevUUID != null) {
        if (ddf.getName() == null) {
          mDDFCache.invalidate(prevUUID);
          mDDFCache.put(uuid, ddf);
        } else {
          mDDFwithNameCache.invalidate(prevUUID);
          mDDFCache.put(uuid, ddf);
        }
        if (ddf.getUri() != null) {
          mUris.put(ddf.getUri(), ddf.getUUID());
        }
      }
    }
  }

  public DDF getDDFByUri(String uri) throws DDFException {
    UUID uuid = this.mUris.get(uri);
    if (uuid == null) {
      throw new DDFException(String.format("Cannot find ddf with uri %s", uri));
    }
    return this.getDDF(uuid);
  }

  private class DDFRemovalListener implements RemovalListener<UUID, DDF> {

    //cleaning up DDF upon removal
    @Override public void onRemoval(RemovalNotification<UUID, DDF> notification) {
      if (notification.wasEvicted()) {
        mLog.info(String.format("CacheStats = %s", getCacheStats().toString()));
        DDF ddf = notification.getValue();
        if (ddf != null) {
          if (ddf.getName() != null) {
            mLog.info(String.format("Removing DDF %s, name = %s", ddf.getUUID(), ddf.getName()));
          } else {
            mLog.info(String.format("Removing DDF %s", ddf.getUUID()));
          }
          ddf.cleanup();
        }
      }
    }
  }

}
