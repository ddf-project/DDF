package io.ddf2.datasource.schema;

public interface IColumn {
	public String getName();
	public Class getType();
	public IFactor getFactor();
}
 
