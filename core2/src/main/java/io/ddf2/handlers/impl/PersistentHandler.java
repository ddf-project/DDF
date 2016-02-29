package io.ddf2.handlers.impl;

import io.ddf2.DDF;

public class PersistentHandler<T extends DDF<T>> implements io.ddf2.handlers.IPersistentHandler<T>{

    @Override
    public boolean persist(T ddf) {
        return false;
    }

    @Override
    public boolean remove(String ddfName) {
        return false;
    }

    @Override
    public T restore(String ddfName) {
        return null;
    }

    @Override
    public long getLastPersistTime(String ddfName) {
        return 0;
    }
}

