package io.ddf2.datasource.filesystem;

import io.ddf2.datasource.IFileFormat;

public class JSonFile implements IFileFormat {

    public boolean firstRowIsHeader() {
        return false;
    }
}
 
