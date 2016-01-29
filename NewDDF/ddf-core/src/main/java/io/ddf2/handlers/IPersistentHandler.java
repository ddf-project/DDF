package io.ddf2.handlers;

import io.ddf2.IDDF;

public interface IPersistentHandler {
 
	public boolean persist(IDDF ddf);
	 
	public boolean remove(String ddfName);
	 
	public IDDF restore(String ddfName);

	public long getLastPersistTime(String ddfName);
}
 
