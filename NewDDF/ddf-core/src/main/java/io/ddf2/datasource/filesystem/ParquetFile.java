package io.ddf2.datasource.filesystem;

import io.ddf2.datasource.IFileFormat;

public class ParquetFile implements IFileFormat {

    public boolean firstRowIsHeader() {
        return false;
    }
}
 
