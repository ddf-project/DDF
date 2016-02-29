package io.ddf2.handlers;

import io.ddf2.DDF;

public interface IPersistentHandler {
 
	public boolean persist(DDF ddf);
	 
	public boolean remove(String ddfName);
	 
	public DDF restore(String ddfName);

	public long getLastPersistTime(String ddfName);
}
 
