package io.ddf.hdfs;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.DataFormat;
import io.ddf.ds.DataSourceCredential;
import io.ddf.exception.DDFException;

import com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Created by jing on 2/22/16.
 */
public class HDFSDDFManager extends DDFManager {
    // File system connector.
    private FileSystem fs = null;

    // Upper limit for content preview.
    private static final int K_LIMIT = 1000;

    private String fsUri = null;

    public HDFSDDFManager(String fsUri) throws DDFException {
        assert !Strings.isNullOrEmpty(fsUri);
        this.fsUri = fsUri;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", fsUri);
            this.fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new DDFException(e);
        }
    }

    /**
     * @brief To check whether the hdfs file already contains header.
     * @param hdfsDDF The uri of the hdfs file (single file).
     * @return True if it has header, otherwise false.
     */
    public Boolean hasHeader(HDFSDDF hdfsDDF) {
        // TODO: Should we do this check in backend?
        return false;
    }

    /**
     * @brief To check whether the ddf is a directory.
     * @param hdfsDDF
     * @return
     */
    public Boolean isDir(HDFSDDF hdfsDDF) throws DDFException {
        String path = hdfsDDF.getPath();
        try {
            FileStatus fileStatus = fs.getFileStatus(new Path(path));
            return fileStatus.isDirectory();
        } catch (IOException e) {
            throw new DDFException(e);
        }
    }

    /**
     * @breif To get the dataformat.
     * @param hdfsDDF
     * @return
     */
    public DataFormat getDataFormat(HDFSDDF hdfsDDF) throws DDFException {
        if (hdfsDDF.getIsDir()) return DataFormat.UNDEF;
        String path = hdfsDDF.getPath();
        String extension = path.substring(path.lastIndexOf(".") + 1);
        return DataFormat.valueOf(extension.toUpperCase());
    }


    /**
     * @brief List all the files (including directories under one path)
     * @param path The path.
     * @return The list of file names
     */
    public List<String> listFiles(String path) throws DDFException {
        List<String> ret = new ArrayList<>();
        try {
            FileStatus[] status = fs.listStatus(new Path(path));
            for (int i = 0; i < status.length; i++){
                ret.add(status[i].getPath().toString());
            }
        } catch (IOException e) {
            throw new DDFException(e);
        }
        return ret;
    }


    /**
     * @brief Create a ddf given path.
     * @param path The path.
     * @return
     */
    public HDFSDDF newDDF(String path) throws DDFException {
        return new HDFSDDF(this, path);
    }

    public HDFSDDF newDDF(String path, String schema) throws DDFException {
        return new HDFSDDF(this, path, schema);
    }


    /**
     * @brief Show the first several rows of the s3ddf.
     * @param hdfsDDF
     * @param limit
     * @return
     * @throws DDFException
     */
    public List<String> head(HDFSDDF hdfsDDF, int limit) throws DDFException {
        if (limit > K_LIMIT) {
            limit = K_LIMIT;
        }

        List<String> rows = new ArrayList<String>();

        int pos = 0;
        String s = null;

        if (!hdfsDDF.getIsDir()) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsDDF.getPath()))))) {
                while ( (s = br.readLine()) != null && pos < limit) {
                    rows.add(s);
                    ++pos;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            RemoteIterator<LocatedFileStatus> files = null;
            try {
                files = fs.listFiles(new Path(hdfsDDF.getPath()), false);
                while (files.hasNext() && pos < limit) {
                    LocatedFileStatus lfs = files.next();
                    try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(
                            fs.open(lfs.getPath())))) {
                        while ( (s = br.readLine()) != null && pos < limit) {
                            rows.add(s);
                            ++pos;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                throw new DDFException(e);
            }
        }
        return rows;
    }

    @Override
    public DDF transfer(UUID fromEngine, UUID ddfuuid) throws DDFException {
        throw new DDFException(new UnsupportedOperationException());
    }

    @Override
    public DDF transferByTable(UUID fromEngine, String tableName) throws DDFException {
        throw new DDFException(new UnsupportedOperationException());
    }

    @Override
    public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
        throw new DDFException(new UnsupportedOperationException());
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
    public DDF copyFrom(DDF fromDDF) throws DDFException {
        return null;
    }

    @Override
    public DDF createDDF(Map<Object, Object> options) throws DDFException {
        return null;
    }

    @Override
    public void validateCredential(DataSourceCredential credential) throws DDFException {

    }

    @Override
    public String getSourceUri() {
        return null;
    }

    @Override
    public String getEngine() {
        return "hdfs";
    }

    public void stop() {
        // TODO: Does s3 connection has to be closed?
    }
}