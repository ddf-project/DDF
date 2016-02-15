package io.ddf2.datasource.schema;

public interface IColumn {
	String getName();
	// TODO: @sang, should we have our own column type instead of using java class. For example, the column type can be
	// blob.
	Class getType();
	IFactor getFactor();
	boolean isNumeric();
	boolean isIntegral();
	boolean isFractional();

	void setFactor(IFactor factor);
}


