package io.ddf2;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.ddf2.bigquery.BQDataSource;
import io.ddf2.bigquery.BigQueryContext;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.SqlDataSource;
import io.ddf2.handlers.IPersistentHandler;

public abstract class DDFManager implements IDDFManager {

	protected IDDFMetaData ddfMetaData;
	protected IPersistentHandler persistentHandler;
	protected final Map mapProperties;

	protected final static AtomicInteger ddfCounter = new AtomicInteger();
	protected final static AtomicInteger ddfManagerCounter = new AtomicInteger();
	protected final String ddfManagerId;
	protected static Map<String,IDDFManager> mapDDFManager = new ConcurrentHashMap<>();
	private static Map<String, IDDF> mapDDF = new ConcurrentHashMap<>();

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
	public static IDDFManager getInstance(String ddfManagerClsName, Map options){
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
			T instance = constructors.newInstance(options);
			mapDDFManager.put(instance.getDDFManagerId(),instance);
			return instance;
		} catch (InstantiationException |IllegalAccessException|InvocationTargetException | NoSuchMethodException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}


	public static final IDDFManager getDDFManagerByUUID(String uuid) throws DDFException{
		IDDFManager manager = mapDDFManager.get(uuid);
		if(manager == null){
			throw new DDFException("Not Found DDFManager With Id=" + uuid);
		}
		return manager;
	}

	public static List<IDDFManager> getAllDDFManager() {
		return new ArrayList<>(mapDDFManager.values());
	}




	@Override
	public final IDDF newDDF(IDataSource ds) throws DDFException {
		return newDDF(generateDDFName(), ds);
	}


	@Override
	public IDDF newDDF(String name, String query) throws DDFException {
		return newDDF(name, BQDataSource.builder().setProjectId(BigQueryContext.getProjectId()).setQuery(query).build());
	}



	@Override
	public IDDF newDDF(String query) throws DDFException {
		return newDDF(generateDDFName(),query);
	}

	@Override
	public final IDDF newDDF(String name, IDataSource ds) throws DDFException {
		if(mapDDF.containsKey(name)){
			throw new DDFException("DDF " + name + "exist");
		}

		IDDF ddf =_newDDF(name, ds);
		mapDDF.put(name,ddf);
		return ddf;
	}


	protected abstract IDDF _newDDF(String name, IDataSource ds) throws DDFException;

	/**
	 * Get an instance of DDF by DDFName.
	 * @param ddfName
	 * @return
	 * @throws DDFException if not exist ddfName
	 */
	public final IDDF getDDF(String ddfName) throws DDFException {
		if(mapDDF.containsKey(ddfName)){
			throw new DDFException("DDF " + ddfName + "exist");
		}
		return mapDDF.get(ddfName);
	}

	@Override
	public ISqlResult sql(String query) throws SQLException {
		return null;
	}

	@Override
	public ISqlResult sql(String query, Map<String, String> options) throws SQLException {
		return null;
	}


	@Override
	public final String getDDFManagerId(){
		return ddfManagerId;
	}

	/**
	 * @see io.ddf2.IDDFManager#getPersistentHandler()
	 */
	public IPersistentHandler getPersistentHandler() {
		if(persistentHandler== null){
			synchronized (DDFManager.class){
				if(persistentHandler== null) persistentHandler= _getPersistentHanlder();
			}
		}
		return persistentHandler;
	}
	protected abstract IPersistentHandler _getPersistentHanlder();

	@Override
	public IDDFMetaData getDDFMetaData() {
		if(ddfMetaData == null){
			synchronized (DDFManager.class){
				if(ddfMetaData == null) ddfMetaData = _getDDFMetaData();
			}
		}
		return ddfMetaData;
	}

	protected abstract IDDFMetaData _getDDFMetaData();
	protected synchronized String generateDDFName() {
		return String.format("ddf_%d_%d_%d", ddfCounter.get(), System.currentTimeMillis(), System.nanoTime() % 10);
	}
}
 
