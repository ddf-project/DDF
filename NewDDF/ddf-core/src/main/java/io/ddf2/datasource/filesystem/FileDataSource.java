package io.ddf2.datasource.filesystem;

import io.ddf2.datasource.DataSource;
import io.ddf2.datasource.fileformat.IFileFormat;

import java.util.ArrayList;
import java.util.List;

public abstract class FileDataSource extends DataSource {

	protected List<String> paths;
	protected IFileFormat fileFormat;
	public FileDataSource(){
		paths = new ArrayList<>();
	}

	public List<String> getPaths(){
		return paths;
	}
	public IFileFormat getFileFormat(){
		return fileFormat;
	}

	public abstract static class Builder<T extends FileDataSource> extends DataSource.Builder<T>{

		public Builder<T> addPath(String path){
			datasource.paths.add(path);
			return  this;
		}
		public Builder<T> addPaths(List<String> paths){
			datasource.paths.addAll(paths);
			return  this;
		}
		public Builder<T> setFileFormat(IFileFormat format){
			datasource.fileFormat = format;
			return this;
		}
	}
	 
}
 
