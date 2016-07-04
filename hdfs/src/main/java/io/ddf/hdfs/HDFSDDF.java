package io.ddf.hdfs;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.ddf.DDF;
import io.ddf.datasource.DataFormat;
import io.ddf.exception.DDFException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jing on 2/22/16.
 */
public class HDFSDDF extends DDF {
    // The format of this s3ddf. If it's a folder, we requires that all the files in the folder should have the same
    // format, otherwise the dataformat will be set to the dataformat of the first file under this folder.
    private DataFormat mDataFormat;

    // Schema String.
    private String mSchemaString;

    private ImmutableList<String> paths;

    private Map<String, String> options;

    /**
     * S3DDF is the ddf for s3. It point to a single S3DDFManager, and every S3DDF is a unqiue mapping to a s3 uri.
     * The schema should store the s3 uri as tablename.
     */
    @Deprecated
    public HDFSDDF(HDFSDDFManager manager, String path, String schema, Map<String, String> options) throws DDFException {
        super(manager, null, null, null, null, null);
        initialize(ImmutableList.of(path), schema, options);
    }

    @Deprecated
    public HDFSDDF(HDFSDDFManager manager, String path, Map<String, String> options) throws DDFException {
        super(manager, null, null, null, null, null);
        initialize(ImmutableList.of(path), null, options);
    }

    public HDFSDDF(HDFSDDFManager manager, String[] paths, String schema, Map<String, String> options) throws DDFException {
        super(manager, null, null, null, null, null);
        initialize(ImmutableList.copyOf(paths), schema, options);
    }

    private void initialize(ImmutableList<String> paths, String schema, Map<String, String> options) throws DDFException {
        this.paths = paths;
        this.mSchemaString = schema;
        this.options = options;

        if (paths.size() == 0) {
            throw new DDFException("No path was specified");
        }
        Iterator<String> i = paths.iterator();
        while (i.hasNext()) {
            String path = i.next();
            if (Strings.isNullOrEmpty(path)) {
                throw new DDFException("One of the paths is empty");
            }
        }
        // Check dataformat.
        mDataFormat = DataFormat.CSV;
        if (options != null && options.containsKey("format")) {
            try {
                String format = options.get("format").toUpperCase();
                format = format.equals("PARQUET") ? "PQT" : format;
                mDataFormat = DataFormat.valueOf(format);
            } catch (IllegalArgumentException e) {
                throw new DDFException(String.format("Unsupported dataformat: %s", options.get("format")));
            }
        }
        mLog.info(String.format("HDFS data format %s", mDataFormat));
    }

    public DataFormat getDataFormat() {
        return mDataFormat;
    }

    public String getSchemaString() {
        return mSchemaString;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public HDFSDDFManager getManager() {
        return (HDFSDDFManager)super.getManager();
    }

    @Override
    public DDF copy() throws DDFException {
        throw new DDFException(new UnsupportedOperationException());
    }

    public List<String> getPaths() {
        String sourceUri = getManager().getSourceUri();
        return paths.stream().map(
                path -> String.format("%s%s", sourceUri, path)
        ).collect(Collectors.toList());
    }
}
