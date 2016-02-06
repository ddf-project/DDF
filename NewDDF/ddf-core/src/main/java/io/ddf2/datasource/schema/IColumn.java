package io.ddf2.datasource.schema;

public interface IColumn {
	String getName();
	Class getType();
	IFactor getFactor();
	boolean isNumeric();
	boolean isIntegral();
	boolean isFractional();
}


