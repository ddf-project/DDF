package io.ddf2.datasource.filesystem;

public class LocalFileDataSource extends FileDataSource {
    /** Only Allow SchemaBuilder To Build New Instance **/
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
 
