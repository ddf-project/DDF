package io.ddf2.handlers;

import io.ddf2.DDF;

public interface IPersistentHandler <T extends DDF<T>>{
 
	public boolean persist(T ddf);
	 
	public boolean remove(String ddfName);
	 
	public DDF restore(String ddfName);

	public long getLastPersistTime(String ddfName);
}
 
