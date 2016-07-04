package io.ddf.s3;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.ddf.DDF;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.S3DataSourceCredentials;
import io.ddf.exception.DDFException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jing on 12/2/15.
 */
public class S3DDF extends DDF {
    // The format of this s3ddf. If it's a folder, we requires that all the files in the folder should have the same
    // format, otherwise the dataformat will be set to the dataformat of the first file under this folder.
    private DataFormat mDataFormat;

    // Schema String.
    private String mSchemaString;

    private ImmutableList<String> paths;

    // Options, including:
    // key : possible values
    // header : true / false
    // format: csv / parquet / etc.
    // delimiter : , \x001
    // quote : "
    // escape : \
    // mode : used for spark
    // charSet:
    // inferSchema:
    // comment:
    // nullvalue:
    // dateformat:
    // flatten : true / false
    private Map<String, String > options;

    /**
     * S3DDF is the ddf for s3. It point to a single S3DDFManager, and every S3DDF is a unqiue mapping to a s3 uri.
     */
    public S3DDF(S3DDFManager manager, String path, Map<String, String> options) throws DDFException {
        super(manager, null, null, null, null, null);
        initialize(ImmutableList.of(path), null, options);
    }

    public S3DDF(S3DDFManager manager, String path, String schema, Map<String, String> options) throws DDFException {
        super(manager, null, null, null, null, null);
        initialize(ImmutableList.of(path), schema, options);
    }

    public S3DDF(S3DDFManager manager, List<String> paths, String schema, Map<String, String> options) throws DDFException {
        super(manager, null, null, null, null, null);
        initialize(ImmutableList.copyOf(paths), schema, options);
    }

    public S3DDF(S3DDFManager manager, String bucket, String key, String schema, Map<String, String> options)
            throws DDFException {
        super(manager, null, null, null, null, null);
        initialize(ImmutableList.of(String.format("%s/%s", bucket, key)), schema, options);
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
        mLog.info(String.format("S3 data format %s", mDataFormat));
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
    public S3DDFManager getManager() {
        return (S3DDFManager)super.getManager();
    }

    @Override
    public DDF copy() throws DDFException {
        throw new DDFException(new UnsupportedOperationException());
    }

    public List<String> getPaths() {
        S3DataSourceCredentials cred = getManager().getCredential();
        return paths.stream().map(
                path -> String.format("s3a://%s:%s@%s", cred.getAwsKeyID(), cred.getAwsScretKey(), path)
        ).collect(Collectors.toList());
    }
}
