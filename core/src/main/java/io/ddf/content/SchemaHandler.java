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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import io.ddf.content.Schema.DummyCoding;

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
  public synchronized void setFactorLevels(String columnName, Factor<?> factor) throws DDFException {
    Column c = this.getColumn(columnName);
    Factor<?> f = c.getOptionalFactor();
    if(factor.getLevels() != null) {
      f.setLevels(factor.getLevels(), false);
    }
  }

  @Override
  public synchronized Factor<?> setAsFactor(String columnName) throws DDFException {
    if (this.getSchema() == null)
      throw new DDFException("Schema is null");

    Column column = this.getSchema().getColumn(columnName);
    if(column == null) {
      throw new DDFException(String.format("Column with name %s does not exist in DDF", columnName), null);
    }
    Factor<?> factor = new Factor.FactorBuilder().setDDF(this.getDDF()).setColumnName(columnName).
        setType(column.getType()).build();

    column.setAsFactor(factor);
    return factor;
  }

  @Override
  public Map<String, Integer> computeLevelCounts(String columnName) throws DDFException {
    return this.computeLevelCounts(new String[]{columnName}).get(columnName);
  }

  @Override
  public Map<String, Map<String, Integer>> computeLevelCounts(String[] columnNames) throws DDFException {
    throw new UnsupportedOperationException();
  }

  protected List<Object> computeFactorLevels(String columnName) throws DDFException {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized Factor<?> setAsFactor(int columnIndex) throws DDFException {
    return this.setAsFactor(this.getColumnName(columnIndex));
  }

  @Override
  public synchronized void unsetAsFactor(String columnName) {
    this.unsetAsFactor(this.getColumnIndex(columnName));
  }

  @Override
  public synchronized void unsetAsFactor(int columnIndex) {
    if (this.getSchema() != null)
      this.getSchema().getColumn(columnIndex).unsetAsFactor();
  }

  @Override
  public DummyCoding getDummyCoding() throws NumberFormatException,
      DDFException {
    DummyCoding dummyCoding = new DummyCoding();
    // initialize array xCols which is just 0, 1, 2 ..
    dummyCoding.xCols = new int[this.getColumns().size()];
    int i = 0;
    while (i < dummyCoding.xCols.length) {
      dummyCoding.xCols[i] = i;
      i += 1;
    }

    List<Column> columns = this.getColumns();
    Iterator<Column> columnIterator = columns.iterator();
    int count = 0;
    while (columnIterator.hasNext()) {
      Column currentColumn = columnIterator.next();
      int currentColumnIndex = this.getColumnIndex(currentColumn.getName());
      HashMap<String, Double> temp = new HashMap<String, java.lang.Double>();
      // loop
      Factor<?> factor = currentColumn.getOptionalFactor();
      if (currentColumn.getColumnClass() == Schema.ColumnClass.FACTOR) {
        if (factor != null) {
          List<Object>  levels = this.computeFactorLevels(currentColumn.getName());
          factor.setLevels(levels);
          Map<String, Integer> levelMaps = factor.getLevelMap();

          Iterator<String> valuesIterator = levelMaps.keySet().iterator();

          //TODO update this code
          int j = 0;
          temp = new HashMap<String, java.lang.Double>();
          while (valuesIterator.hasNext()) {
            String columnValue = valuesIterator.next();
            temp.put(columnValue, Double.parseDouble(j + ""));
            j += 1;
          }
          dummyCoding.getMapping().put(currentColumnIndex, temp);
          count += temp.size() - 1;
        }
      }
    }
    dummyCoding.setNumDummyCoding(count);


    // ignore Y column
    Integer numFeatures = this.getNumColumns() - 1;
    // plus bias term for linear model
    numFeatures += 1;
    // plus the new dummy coding columns
    numFeatures += dummyCoding.getNumDummyCoding();

    //dc.getMapping().size() means number of factor column
    //numFeatures -= (!dummyCoding.getMapping().isEmpty()) ? dummyCoding.getMapping().size() : 0;
    if(!dummyCoding.getMapping().isEmpty()) {
      numFeatures -= dummyCoding.getMapping().size();
    }
    dummyCoding.setNumberFeatures(numFeatures);
    // set number of features in schema
    return dummyCoding;
  }
}
