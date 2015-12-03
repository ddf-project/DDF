package io.ddf.s3;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

/**
 * Created by jing on 12/2/15.
 */
public class S3DDF extends DDF {
    Boolean mHasHeader = false;
    /**
     * S3DDF is the ddf for s3. It point to a single S3DDFManager, and every S3DDF is a unqiue mapping to a s3 uri.
     * The schema should store the s3 uri as tablename.
     */

    /**
     * @brief Constructors.
     * @param manager
     * @param data
     * @param typeSpecs
     * @param namespace
     * @param name
     * @param schema
     * @throws DDFException
     */
    public S3DDF(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema)
        throws DDFException {
        super(manager, data, typeSpecs, namespace, name, schema);
    }

    protected S3DDF(DDFManager manager, DDFManager defaultManagerIfNull) throws DDFException {
        super(manager, defaultManagerIfNull);
    }

    protected S3DDF(DDFManager manager) throws DDFException {
        super(manager);
    }

    protected S3DDF() throws DDFException {
        super();
    }

    public Boolean getmHasHeader() {
        return mHasHeader;
    }

    public void setmHasHeader(Boolean mHasHeader) {
        this.mHasHeader = mHasHeader;
    }

    @Override
    public DDF copy() throws DDFException {
        return null;
    }
}
