package io.ddf;


import com.google.common.base.Strings;
import com.google.common.cache.*;
import io.ddf.exception.DDFException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.ddf.misc.ALoggable;
import io.ddf.misc.Config;

import java.util.concurrent.TimeUnit;

/**
 * Created by huandao on 6/11/15.
 */
public class DDFCache extends ALoggable {

  private LoadingCache<UUID, DDF> mDDFCache;

  private DDFManager mDDFManager;

  public DDFCache(DDFManager manager) {
    mDDFManager = manager;
    Long maxNumberOfDDFs = Long.valueOf(Config.getGlobalValue(Config.ConfigConstant.MAX_NUMBER_OF_DDFS_IN_CACHE));
    Long ddfExpiredTime = Long.valueOf(Config.getGlobalValue(Config.ConfigConstant.DDF_EXPIRED_TIME));
    mLog.info(String.format("Maximum number of ddfs in cache %s", maxNumberOfDDFs));
    mDDFCache = CacheBuilder.newBuilder().
        maximumSize(maxNumberOfDDFs).recordStats().
        expireAfterAccess(ddfExpiredTime, TimeUnit.SECONDS) //.removalListener(new DDFRemovalListener())
        .build(new CacheLoader<UUID, DDF>() {
      @Override public DDF load(UUID uuid) throws Exception {
        try {
          mLog.info(String.format("restoring ddf %s", uuid));
          return mDDFManager.restoreDDF(uuid);
        } catch (Exception e) {
          throw new DDFException(String.format("DDF with uuid %s does not exist", uuid));
        }
      }
    });
  }

  private boolean containsDDF(UUID uuid) {
    return mDDFCache.asMap().containsKey(uuid);
  }

  private Map<UUID, DDF> getEntries() {
    return mDDFCache.asMap();
  }

//  private Map<UUID, DDF> mDDFs = new ConcurrentHashMap<UUID, DDF>();
  private Map<String, UUID> mUris = new ConcurrentHashMap<String, UUID>();

  public void addDDF(DDF ddf) throws DDFException {
    if(ddf.getUUID() == null) {
      throw new DDFException("uuid is null");
    }
    mDDFCache.put(ddf.getUUID(), ddf);
  }

  public CacheStats getCacheStats() {
    return mDDFCache.stats();
  }

  public void removeDDF(DDF ddf) throws DDFException {
    mDDFCache.invalidate(ddf.getUUID());

    if(ddf.getUri() != null) {
      mUris.remove(ddf.getUri());
    }
  }

  public DDF[] listDDFs() {
    return mDDFCache.asMap().values().toArray(new DDF[]{});
  }

  public DDF getDDF(UUID uuid) throws DDFException {
    return mDDFCache.getUnchecked(uuid);
  }

  public DDF getDDFByName(String name) throws DDFException {
    for(DDF ddf: this.listDDFs()) {
      if(!Strings.isNullOrEmpty(ddf.getName()) && ddf.getName().equals(name)) {
        return ddf;
      }
    }
    throw new DDFException(String.format("Cannot find ddf with name %s", name));
  }

  public synchronized void setDDFName(DDF ddf, String name) throws DDFException {
    if(!Strings.isNullOrEmpty(name)) {
      if(!Strings.isNullOrEmpty(ddf.getName())) {
        this.mUris.remove(ddf.getUri());
      }
      ddf.setName(name);
      this.mUris.put(ddf.getUri(), ddf.getUUID());
    } else {
      throw new DDFException(String.format("DDF's name cannot be null or empty"));
    }
  }

  public synchronized void setDDFUUID(DDF ddf, UUID uuid) throws DDFException {
    if(this.containsDDF(uuid)) {
      throw new DDFException(String.format("DDF with uuid %s already exists", uuid));
    } else {
      UUID prevUUID = ddf.getUUID();
      ddf.setUUID(uuid);
      if(prevUUID != null) {
        mDDFCache.invalidate(prevUUID);
        mDDFCache.put(uuid, ddf);
        if(ddf.getUri() != null) {
          mUris.remove(ddf.getUri());
          mUris.put(ddf.getUri(), ddf.getUUID());
        }
      }
    }
  }

  public DDF getDDFByUri(String uri) throws DDFException {
    UUID uuid = this.mUris.get(uri);
    if(uuid == null) {
      throw new DDFException(String.format("Cannot find ddf with uri %s", uri));
    }
    return this.getDDF(uuid);
  }

  private class DDFRemovalListener implements RemovalListener<UUID, DDF> {

    //cleaning up DDF upon removal
    @Override
    public void onRemoval(RemovalNotification<UUID, DDF> notification) {
      if(notification.wasEvicted()) {
        mLog.info(String.format("CacheStats = %s", getCacheStats().toString()));
        DDF ddf = notification.getValue();
        if (ddf != null) {
          mLog.info(String.format("Removing DDF %s", ddf.getUUID()));
          ddf.cleanup();
        }
      }
    }
  }
}
