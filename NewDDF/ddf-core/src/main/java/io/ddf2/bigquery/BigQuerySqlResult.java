package io.ddf2.bigquery;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import io.ddf2.IDDFType;
import io.ddf2.ISqlResult;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.datasource.schema.Schema;

import java.io.Closeable;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * Created by sangdn on 1/20/16.
 */
public class BigQuerySqlResult implements ISqlResult{
    protected GetQueryResultsResponse bqResultResponse;
    protected ISchema schema;
    public BigQuerySqlResult(GetQueryResultsResponse bqResultsResponse){
        this.bqResultResponse = bqResultsResponse;
        schema = convert(bqResultsResponse.getSchema());
    }
    protected ISchema convert(TableSchema tableSchema){
        List<TableFieldSchema> fields = tableSchema.getFields();
        Schema.SchemaBuilder builder = Schema.builder();
        for(TableFieldSchema field: fields){
            builder.add(field.getName(),field.getType());
        }

    }
    @Override
    public ISchema getSchema() {

        return null;
    }

    @Override
    public void first() {

    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public Object getRaw() {
        return null;
    }

    @Override
    public Object get(int index) {
        return null;
    }

    @Override
    public Object get(String name) {
        return null;
    }

    @Override
    public Object get(int index, Object defaultValue) {
        return null;
    }

    @Override
    public Object get(String name, Object defaultValue) {
        return null;
    }

    @Override
    public int getInt(int index) {
        return 0;
    }

    @Override
    public int getInt(String name) {
        return 0;
    }

    @Override
    public int getInt(int index, int defaultValue) {
        return 0;
    }

    @Override
    public int getInt(String name, int defaultValue) {
        return 0;
    }

    @Override
    public Boolean getBoolean(int index) {
        return null;
    }

    @Override
    public Boolean getBoolean(String name) {
        return null;
    }

    @Override
    public Boolean getBoolean(int index, Boolean defaultValue) {
        return null;
    }

    @Override
    public Boolean getBoolean(String name, Boolean defaultValue) {
        return null;
    }

    @Override
    public long getLong(int index) {
        return 0;
    }

    @Override
    public long getLong(String name) {
        return 0;
    }

    @Override
    public long getLong(int index, long defaultValue) {
        return 0;
    }

    @Override
    public long getLong(String name, long defaultValue) {
        return 0;
    }

    @Override
    public double getDouble(int index) {
        return 0;
    }

    @Override
    public double getDouble(String name) {
        return 0;
    }

    @Override
    public long getDouble(int index, long defaultValue) {
        return 0;
    }

    @Override
    public double getDouble(String name, double defaultValue) {
        return 0;
    }

    @Override
    public Date getDate(int index) throws ParseException {
        return null;
    }

    @Override
    public Date getDate(String name) {
        return null;
    }

    @Override
    public Date getDate(int index, Date defaultValue) {
        return null;
    }

    @Override
    public Date getDate(String name, Date defaultValue) {
        return null;
    }

    @Override
    public String getString(int index) {
        return null;
    }

    @Override
    public String getString(String name) {
        return null;
    }

    @Override
    public String getString(int index, String defaultValue) {
        return null;
    }

    @Override
    public String getString(String name, String defaultValue) {
        return null;
    }

    @Override
    public <T extends IDDFType> T getType(int index, Class<T> cls) throws SQLException {
        return null;
    }

    @Override
    public <T extends IDDFType> T getType(String name, Class<T> cls) {
        return null;
    }

    @Override
    public <T extends IDDFType> T getType(int index, Class<T> cls, T defaultValue) {
        return null;
    }

    @Override
    public <T extends IDDFType> T getType(String name, Class<T> cls, T defaultValue) {
        return null;
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     * <p>
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     * <p>
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     * <p>
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     * <p>
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {

    }
}
