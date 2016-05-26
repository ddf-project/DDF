package io.ddf;


import com.google.common.base.Strings;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.ddf.exception.DDFException;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.cache.CacheBuilder;
import io.ddf.misc.Config;

import java.util.concurrent.TimeUnit;

/**
 * Created by huandao on 6/11/15.
 */
public class DDFCache {

  private LoadingCache<UUID, DDF> mDDFCache;

  private DDFManager mDDFManager;
  public DDFCache(DDFManager manager) {
    mDDFManager = manager;
    Long maxNumberOfDDFs = Long.valueOf(Config.getGlobalValue(Config.ConfigConstant.MAX_NUMBER_OF_DDFS_IN_CACHE));
    mDDFCache = CacheBuilder.newBuilder().
        maximumSize(maxNumberOfDDFs).expireAfterAccess(4, TimeUnit.HOURS).build(new CacheLoader<UUID, DDF>() {
      @Override public DDF load(UUID uuid) throws Exception {
        return mDDFManager.restoreDDF(uuid);
      }
    });
  }
//  private Map<UUID, DDF> mDDFs = new ConcurrentHashMap<UUID, DDF>();
  private Map<String, UUID> mUris = new ConcurrentHashMap<String, UUID>();

  public void addDDF(DDF ddf) throws DDFException {
    mDDFCache.put(ddf.getUUID(), ddf);
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
    try {
      this.getDDF(uuid);
      throw new DDFException(String.format("DDF with uuid %s already exists", uuid));
    } catch(DDFException exception) {
      UUID prevUUID = ddf.getUUID();
      if(prevUUID != null) {
        mDDFCache.invalidate(prevUUID);
        ddf.setUUID(uuid);
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
}
