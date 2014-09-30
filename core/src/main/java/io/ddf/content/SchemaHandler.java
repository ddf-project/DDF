/**
 * 
 */
package io.ddf.content;


import java.sql.Timestamp;
import java.util.List;
import io.ddf.DDF;
import io.ddf.Factor;
import io.ddf.content.Schema.Column;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.ddf.util.DDFUtils;

/**
 */
public class SchemaHandler extends ADDFFunctionalGroupHandler implements IHandleSchema {

  public SchemaHandler(DDF theDDF) {
    super(theDDF);
  }


  private Schema mSchema;


  @Override
  public Schema getSchema() {
    return mSchema;
  }

  @Override
  public void setSchema(Schema theSchema) {
    this.mSchema = theSchema;
  }

  /**
   * @return the Schema's table name
   */
  @Override
  public String getTableName() {
    return mSchema != null ? mSchema.getTableName() : null;
  }

  @Override
  public Column getColumn(String columnName) {
    return mSchema.getColumn(columnName);
  }

  @Override
  public List<Column> getColumns() {
    return mSchema != null ? mSchema.getColumns() : null;
  }

  @Override
  public String newTableName() {
    return newTableName(this.getDDF());
  }

  @Override
  public String newTableName(Object obj) {
    return DDFUtils.generateObjectName(obj);
  }

  @Override
  public int getNumColumns() {
    return mSchema != null ? mSchema.getNumColumns() : -1;
  }

  @Override
  public int getColumnIndex(String columnName) {
    return mSchema != null ? mSchema.getColumnIndex(columnName) : -1;
  }

  @Override
  public String getColumnName(int columnIndex) {
    return mSchema != null ? mSchema.getColumnName(columnIndex) : null;
  }

  @Override
  public Schema generateSchema() throws DDFException {
    if (this.getSchema() != null) return this.getSchema();

    // Try to infer from the DDF's data
    Object data = this.getDDF().getRepresentationHandler().getDefault();

    // TODO: for now, we'll just support the "null" case
    if (data == null) return new Schema(null, "null BLOB");

    return null;
  }

  @Override
  public void setFactorLevelsForStringColumns(String[] xCols) throws DDFException {

  }

  @Override
  public void computeFactorLevelsAndLevelCounts() throws DDFException {

  }



  @Override
  public Factor<?> setAsFactor(String columnName) {
    if (this.getSchema() == null) return null;

    Factor<?> factor = null;

    Column column = this.getSchema().getColumn(columnName);
    switch (column.getType()) {
      case DOUBLE:
        factor = new Factor<Double>(this.getDDF(), columnName);
        break;
      case FLOAT:
        factor = new Factor<Float>(this.getDDF(), columnName);
        break;
      case INT:
        factor = new Factor<Integer>(this.getDDF(), columnName);
        break;
      case LONG:
        factor = new Factor<Long>(this.getDDF(), columnName);
        break;
      case LOGICAL:
        factor = new Factor<Boolean>(this.getDDF(), columnName);
        break;
      case STRING:
        factor = new Factor<String>(this.getDDF(), columnName);
        break;
      case TIMESTAMP:
        factor = new Factor<Timestamp>(this.getDDF(), columnName);
        break;
      case BLOB:
      default:
        factor = new Factor<Object>(this.getDDF(), columnName);
        break;
    }

    column.setAsFactor(factor);

    return factor;
  }

  public void generateDummyCoding() throws NumberFormatException, DDFException {
    this.getSchema().generateDummyCoding();
  }

  @Override
  public Factor<?> setAsFactor(int columnIndex) {
    return this.setAsFactor(this.getColumnName(columnIndex));
  }

  @Override
  public void unsetAsFactor(String columnName) {
    this.unsetAsFactor(this.getColumnIndex(columnName));
  }

  @Override
  public void unsetAsFactor(int columnIndex) {
    if (this.getSchema() != null) this.getSchema().getColumn(columnIndex).unsetAsFactor();
  }

  public void computeFactorLevelsForAllStringColumns() throws DDFException {
    // TODO Auto-generated method stub

  }

}
