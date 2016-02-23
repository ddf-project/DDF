package io.ddf2.spark;

import io.ddf2.IDDFMetaData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.ddf2.ISqlResult;
import io.ddf2.datasource.schema.ISchema;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class SparkDDFMetadata implements IDDFMetaData {

	protected HiveContext hiveContext;
	public SparkDDFMetadata(HiveContext hiveContext){
		this.hiveContext = hiveContext;
	}
	/**
	 * @see io.ddf2.IDDFMetaData#getAllDDFNames()
	 */
	public Set<String> getAllDDFNames() {

		DataFrame df = hiveContext.sql("show tables");
		ISqlResult sqlResult = SparkUtils.dataFrameToSqlResult(df);
		Set<String> ddfNames  = new HashSet<>();
		while(sqlResult.next()){
			ddfNames.add(sqlResult.getString(0));
		}
		return ddfNames;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#getAllDDFNameWithSchema()
	 */
	public Set<Pair<String,ISchema>>getAllDDFNameWithSchema() {
		return null;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#getDDFSchema(java.lang.String)
	 */
	public ISchema getDDFSchema(String ddfName) {
		return null;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#dropAllDDF()
	 */
	public int dropAllDDF() {
		Set<String> ddfNames = getAllDDFNames();
		ddfNames.forEach(ddfName -> dropDDF(ddfName));
		return ddfNames.size();

	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#getNumDDF()
	 */
	public int getNumDDF() {
		return getAllDDFNames().size();
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#dropDDF(java.lang.String)
	 */
	public boolean dropDDF(String ddfName) {
		try {
			DataFrame sql = hiveContext.sql("drop table if exists " + ddfName);
			return sql != null;
		}catch (Exception ex){
			return false;
		}

	}

	@Override
	public long getCreationTime(String ddfName) {
		return 0;
	}

}
 
