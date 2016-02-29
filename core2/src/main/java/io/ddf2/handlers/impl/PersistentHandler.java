package io.ddf2.handlers.impl;

import io.ddf2.DDF;

public class PersistentHandler  implements io.ddf2.handlers.IPersistentHandler{

    @Override
    public boolean persist(DDF ddf) {
        return false;
    }

    @Override
    public boolean remove(String ddfName) {
        return false;
    }

    @Override
    public DDF restore(String ddfName) {
        return null;
    }

    @Override
    public long getLastPersistTime(String ddfName) {
        return 0;
    }
}

