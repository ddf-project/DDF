package io.ddf2.handlers;

/**
 * Created by jing on 1/25/16.
 */


import io.ddf2.DDF;
import io.ddf2.DDFException;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public interface IHandleMetaData extends IDDFHandler {

    public UUID getId();

    public void setId(UUID id);

    public long getNumRows() throws DDFException;

    public void copyFactor(DDF ddf)  throws DDFException;

    public void copyFactor(DDF ddf, List<String> colums)  throws DDFException;

    public void copy(IHandleMetaData otherMetaData) throws DDFException;

    public void setLastRefreshTime(Date time);

    public Date getLastRefreshTime();

    public void setLastRefreshUser(String user);

    public String getLastRefreshUser();

    public void setLastModifiedTime(Date time);

    public Date getLastModifiedTime();

    public void setLastModifiedUser(String user);

    public String getLastModifiedUser();

    public void setLastPersistedTime(Date time);

    public Date getLastPersistedTime();

    // return true if ddf is in use
    // false otherwise
    public boolean inUse();

    //increase number of user using ddf
    public void increaseUseCount();
}

