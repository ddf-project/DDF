package io.ddf2.datasource.filesystem;

import io.ddf2.datasource.DataSource;
import io.ddf2.datasource.fileformat.IFileFormat;

import java.util.List;

public abstract class FileDataSource extends DataSource {

	public abstract List<String> getPaths();
	public abstract IFileFormat getFileFormat();

	public class Builder<T extends FileDataSource>{

	}
	 
}
 
