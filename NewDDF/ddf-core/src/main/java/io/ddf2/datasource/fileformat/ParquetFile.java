package io.ddf2.datasource.fileformat;

public class ParquetFile implements IFileFormat {

    public boolean firstRowIsHeader() {
        return false;
    }
}
 
