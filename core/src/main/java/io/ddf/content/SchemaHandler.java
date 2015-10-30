/**
 *
 */
package io.ddf.content;


import io.ddf.DDF;
import io.ddf.Factor;
import io.ddf.content.Schema.Column;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.ddf.util.DDFUtils;

import java.sql.Timestamp;
import java.util.List;

/**
 */
public class SchemaHandler extends ADDFFunctionalGroupHandler implements
    IHandleSchema {

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
    if (this.getSchema() != null)
      return this.getSchema();

    // Try to infer from the DDF's data
    Object data = this.getDDF().getRepresentationHandler().getDefault();

    // TODO: for now, we'll just support the "null" case
    if (data == null)
      return new Schema(null, "null BLOB");

    return null;
  }

  @Override
  public void setFactorLevelsForStringColumns(String[] xCols) throws DDFException {

  }

  @Override
  public void setFactorLevels(String columnName, Factor<?> factor) throws DDFException {
    Column c = this.getColumn(columnName);
    Factor<?> f = c.getOptionalFactor();
    if(factor.getLevelCounts() != null) {
      f.setLevelCounts(factor.getLevelCounts());
    }
    if(factor.getLevels() != null) {
      f.setLevels(factor.getLevels(), false);
    }
  }

  @Override
  public void computeFactorLevelsAndLevelCounts() throws DDFException {

  }



  @Override
  public Factor<?> setAsFactor(String columnName) throws DDFException {
    if (this.getSchema() == null)
      return null;

    Factor<?> factor = null;

    Column column = this.getSchema().getColumn(columnName);
    if(column == null) {
      throw new DDFException(String.format("Column with name %s does not exist in DDF", columnName), null);
    }
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
      case BIGINT:
        factor = new Factor<Long>(this.getDDF(), columnName);
        break;
      case BOOLEAN:
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

  public void generateDummyCoding() throws NumberFormatException,
      DDFException {
    this.getSchema().generateDummyCoding();
  }

  @Override
  public Factor<?> setAsFactor(int columnIndex) throws DDFException {
    return this.setAsFactor(this.getColumnName(columnIndex));
  }

  @Override
  public void unsetAsFactor(String columnName) {
    this.unsetAsFactor(this.getColumnIndex(columnName));
  }

  @Override
  public void unsetAsFactor(int columnIndex) {
    if (this.getSchema() != null)
      this.getSchema().getColumn(columnIndex).unsetAsFactor();
  }

  public void computeFactorLevelsForAllStringColumns() throws DDFException {
    // TODO Auto-generated method stub

  }

}
