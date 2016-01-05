package io.ddf2.datasource.filesystem;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.fileformat.IFileFormat;

import java.util.List;

public class LocalFileDataSource extends FileDataSource {
    /** Only Allow Builder To Build New Instance **/
    private LocalFileDataSource(){}


    public static Builder<LocalFileDataSource> builder() {
        return new Builder<LocalFileDataSource>() {
            @Override
            protected LocalFileDataSource newInstance() {
                return new LocalFileDataSource();
            }
        };

    }
}
 
