package io.ddf2.datasource.filesystem.fileformat;

public class ParquetFile implements IFileFormat {

    public boolean firstRowIsHeader() {
        return false;
    }
}
 
