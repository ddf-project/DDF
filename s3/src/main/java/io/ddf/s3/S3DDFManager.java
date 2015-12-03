package io.ddf.s3;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.exception.DDFException;

import java.util.List;
import java.util.UUID;

/**
 * Created by jing on 12/2/15.
 */

public class S3DDFManager extends DDFManager {
    DataSourceDescriptor mDsd;

    public S3DDFManager(DataSourceDescriptor s3dsd) throws DDFException {
        mDsd = s3dsd;
    }

    /**
     * @brief To check whether the s3 file already contains header.
     * @param s3uri The uri of the s3 file (single file).
     * @return True if it has header, otherwise false.
     */
    public Boolean hasHeader(String s3uri) {
        return false;
    }


    /**
     * @brief List all the files (including directories under one path)
     * @param path The path.
     * @return The list of file names (TODO: should we return more info here.)
     */
    public List<String> listFiles(String path) {
        return null;
    }

    /**
     * @brief Create a ddf given path.
     * @param path The path.
     * @return
     */
    public DDF newDDF(String path) {
        return null;
    }

    @Override
    public DDF transfer(UUID fromEngine, UUID ddfuuid) throws DDFException {
        return null;
    }

    @Override
    public DDF transferByTable(UUID fromEngine, String tableName) throws DDFException {
        return null;
    }

    @Override
    public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
        return null;
    }

    @Override
    public DDF getOrRestoreDDFUri(String ddfURI) throws DDFException {
        return null;
    }

    @Override
    public DDF getOrRestoreDDF(UUID uuid) throws DDFException {
        return null;
    }

    @Override
    public String getEngine() {
        return "s3";
    }
}