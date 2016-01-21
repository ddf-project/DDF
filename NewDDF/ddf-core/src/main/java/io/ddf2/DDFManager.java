package io.ddf2;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.ddf2.datasource.IDataSource;
import io.ddf2.handlers.IPersistentHandler;

public abstract class DDFManager implements IDDFManager {

	protected IDDFMetaData ddfMetaData;
	protected IPersistentHandler persistentHandler;
	protected final Map mapProperties;

	protected final static AtomicInteger ddfCounter = new AtomicInteger();
	protected final static AtomicInteger ddfManagerCounter = new AtomicInteger();
	protected final String ddfManagerId;
	protected DDFManager(Map mapProperties){
		this.mapProperties = new HashMap<>();
		if(mapProperties != null && mapProperties.size()>0){
			this.mapProperties.putAll(mapProperties);
		}
		ddfManagerId = "DDFManager_" + ddfManagerCounter.incrementAndGet();

	}

	/*
	 	Get concrete DDFManager from className
	 */
	public static DDFManager getInstance(String ddfManagerClsName, Map options){
		try {
			Class cls = Class.forName(ddfManagerClsName);
			if(cls.isAssignableFrom(DDFManager.class)){
				return getInstance(cls,options);
			}else{
				throw new IllegalArgumentException("Class " + ddfManagerClsName + " Must extend from DDFManager");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}

	}
	/* get concrete DDFManager for its class */
	public static  <T extends DDFManager> T getInstance(Class<T> ddfManager, Map options){
		try {
			Constructor<T> constructors = ddfManager.getDeclaredConstructor(Map.class);
			constructors.setAccessible(true);
			return constructors.newInstance(options);
		} catch (InstantiationException |IllegalAccessException|InvocationTargetException | NoSuchMethodException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}


	public static final DDFManager getDDFManagerByUUID(String uuid) {
		throw new RuntimeException("Not Implement Yet");
	}

	public static List<DDFManager> getAllDDFManager() {
		throw new RuntimeException("Not Implement Yet");
	}

	public static DDF getDDF(String uuid) {
		throw new RuntimeException("Not Implement Yet");
	}


	@Override
	public final IDDF newDDF(IDataSource ds) throws DDFException {
		return newDDF(generateDDFName(), ds);
	}

	@Override
	public final IDDF newDDF(String name, IDataSource ds) throws DDFException {
		return _newDDF(name, ds);
	}


	protected abstract IDDF _newDDF(String name, IDataSource ds) throws DDFException;

	@Override
	public final String getDDFManagerId(){
		return ddfManagerId;
	}

	/**
	 * @see io.ddf2.IDDFManager#getPersistentHandler()
	 */
	public IPersistentHandler getPersistentHandler() {
		if(persistentHandler== null){
			synchronized (persistentHandler){
				if(persistentHandler== null) persistentHandler= _getPersistentHanlder();
			}
		}
		return persistentHandler;
	}
	protected abstract IPersistentHandler _getPersistentHanlder();

	@Override
	public IDDFMetaData getDDFMetaData() {
		if(ddfMetaData == null){
			synchronized (ddfMetaData){
				if(ddfMetaData == null) ddfMetaData = _getDDFMetaData();
			}
		}
		return ddfMetaData;
	}

	protected abstract IDDFMetaData _getDDFMetaData();
	protected synchronized String generateDDFName() {
		return String.format("ddf_%d_%ld_%d", ddfCounter.get(), System.currentTimeMillis(), System.nanoTime() % 10);
	}
}
 
