package io.ddf2.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import io.ddf2.*;
import io.ddf2.bigquery.preparer.BigQueryPreparer;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.SqlDataSource;
import org.apache.commons.lang.NotImplementedException;

import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.WeakReference;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sangdn on 1/18/16.
 */
public class BigQueryDDF extends DDF {

    protected String projectId;
    protected String query;
    protected Bigquery bigquery;
    protected String datasetId;
    protected BigQueryDDF(IDataSource dataSource) {
        super(dataSource);
        bigquery = BigQueryUtils.newInstance();
    }



    /***
     * Init @mapDataSourcePreparer.
     * Add all supported DataSource to @mapDataSourcePreparer
     */
    @Override
    protected void initDSPreparer() {
        mapDataSourcePreparer = new HashMap<>();
        mapDataSourcePreparer.put(BQDataSource.class,new BigQueryPreparer(bigquery));
    }

    /**
     * An reserve-function for concrete DDF to hook to build progress.
     */
    @Override
    protected void endBuild() {
        if(this.dataSource instanceof BQDataSource){
            this.numRows = ((BQDataSource)dataSource).getNumRows();
            this.projectId = ((BQDataSource) dataSource).getProjectId();
            this.query = ((BQDataSource)dataSource).getQuery();
            this.datasetId = ((BQDataSource)dataSource).getDatasetId();
        }
    }

    /**
     * @param sql
     * @see IDDF#sql(String)
     */
    @Override
    public ISqlResult sql(String sql) throws SQLException {
        try {
            QueryResponse queryResponse = bigquery.jobs().query(projectId, new QueryRequest().setQuery(sql)).execute();
            return new BigQuerySqlResult(queryResponse);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SQLException("Unable to excute bigquery msg:" + e.getMessage());
        }
    }

    @Override
    public ISqlResult sql(String sql, Map<String, String> options) throws SQLException {
        return sql(sql);
    }

    @Override
    public IDDF sql2ddf(String sql, Map<String, String> options) throws DDFException {
        return sql2ddf(sql);
    }

    @Override
    public IDDF sql2ddf(String sql) throws DDFException {
        BQDataSource bqDataSource = BQDataSource.builder().setProjectId(projectId).setQuery(sql).build();
        return ddfManager.newDDF(bqDataSource);
    }

    @Override
    protected long _getNumRows() {
        return numRows;
    }


    /**
     * @see IDDF#getDDFName()
     */
    @Override
    public String getDDFName() {
        return datasetId + "." + name;
    }

    /**
     * Called by the garbage collector on an object when garbage collection
     * determines that there are no more references to the object.
     * A subclass overrides the {@code finalize} method to dispose of
     * system resources or to perform other cleanup.
     * <p>
     * The general contract of {@code finalize} is that it is invoked
     * if and when the Java&trade; virtual
     * machine has determined that there is no longer any
     * means by which this object can be accessed by any thread that has
     * not yet died, except as a result of an action taken by the
     * finalization of some other object or class which is ready to be
     * finalized. The {@code finalize} method may take any action, including
     * making this object available again to other threads; the usual purpose
     * of {@code finalize}, however, is to perform cleanup actions before
     * the object is irrevocably discarded. For example, the finalize method
     * for an object that represents an input/output connection might perform
     * explicit I/O transactions to break the connection before the object is
     * permanently discarded.
     * <p>
     * The {@code finalize} method of class {@code Object} performs no
     * special action; it simply returns normally. Subclasses of
     * {@code Object} may override this definition.
     * <p>
     * The Java programming language does not guarantee which thread will
     * invoke the {@code finalize} method for any given object. It is
     * guaranteed, however, that the thread that invokes finalize will not
     * be holding any user-visible synchronization locks when finalize is
     * invoked. If an uncaught exception is thrown by the finalize method,
     * the exception is ignored and finalization of that object terminates.
     * <p>
     * After the {@code finalize} method has been invoked for an object, no
     * further action is taken until the Java virtual machine has again
     * determined that there is no longer any means by which this object can
     * be accessed by any thread that has not yet died, including possible
     * actions by other objects or classes which are ready to be finalized,
     * at which point the object may be discarded.
     * <p>
     * The {@code finalize} method is never invoked more than once by a Java
     * virtual machine for any given object.
     * <p>
     * Any exception thrown by the {@code finalize} method causes
     * the finalization of this object to be halted, but is otherwise
     * ignored.
     *
     * @throws Throwable the {@code Exception} raised by this method
     * @jls 12.6 Finalization of Class Instances
     * @see WeakReference
     * @see PhantomReference
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        bigquery.tables().delete(projectId,datasetId,name).executeUnparsed();
    }

    protected abstract static class BigQueryDDFBuilder<T extends BigQueryDDF> extends DDFBuilder<T> {
        public BigQueryDDFBuilder(IDataSource dataSource) {
            super(dataSource);
        }
    }
    public static BigQueryDDFBuilder<?> builder(IDataSource dataSource){
        return new BigQueryDDFBuilder<BigQueryDDF>(dataSource) {
            @Override
            public BigQueryDDF newInstance(IDataSource ds) {
                return new BigQueryDDF(ds);
            }

            @Override
            protected BigQueryDDF newInstance(String query) {
                return newInstance(BQDataSource.builder()
                                                .setProjectId((String) BigQueryContext.getProperty(BigQueryContext.KEY_PROJECT_ID))
                                                .setQuery(query)
                                                .build());
            }

            @Override
            public BigQueryDDF build() throws DDFException {
                ddf.build(mapProperties);
                return ddf;
            }
        };
    }
}
