package io.ddf2.datasource.filesystem;

public class ParquetFile implements IFileFormat {

    public boolean firstRowIsHeader() {
        return false;
    }
}
 
