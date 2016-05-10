package io.ddf.content;


import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.annotations.Expose;
import io.ddf.Factor;
import io.ddf.exception.DDFException;
import io.ddf.util.Utils;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Table schema of a DDF including table name and column metadata
 */
@SuppressWarnings("serial")
public class Schema implements Serializable {
  @Expose
  private String mTableName;
  @Expose
  private List<Column> mColumns = Collections
      .synchronizedList(new ArrayList<Column>());

  private DummyCoding dummyCoding;

  /**
   * Constructor that can take a list of columns in the following format:
   * "<name> <type>, <name> <type>". For example,
   * "id string, description string, units integer, unit_price float, total float"
   * . This string will be parsed into a {@link List} of {@link Column}s.
   *
   * @param columns
   */
  @Deprecated
  // Require tableName at all times, even null
  public Schema(String columns) throws DDFException {
    this.initialize(null, this.parseColumnList(columns));
  }

  /**
   * Constructor that can take a list of columns in the following format:
   * "<name> <type>, <name> <type>". For example,
   * "id string, description string, units integer, unit_price float, total float"
   * .
   *
   * @param tableName
   * @param columns
   */
  public Schema(String tableName, String columns) throws DDFException {
    this.initialize(tableName, this.parseColumnList(columns));
  }

  @Deprecated
  // Require tableName at all times, even null
  public Schema(List<Column> columns) throws DDFException {
    this.initialize(null, columns);
  }

  public Schema(String tableName, List<Column> columns) throws DDFException {
    this.initialize(tableName, columns);
  }

  @SuppressWarnings("unchecked")
  public Schema(String tableName, Column[] columns) throws DDFException {
    this.initialize(tableName, Arrays.asList(columns));
  }

  private void initialize(String tableName, List<Column> columns) throws DDFException {
    this.setColumns(columns);
    this.mTableName = tableName;
  }

  public static void validateSchema(Schema schema) throws DDFException {
    if(schema != null && schema.getColumns() != null) {
      validateColumnNames(schema.getColumnNames());
    }
  }

  private static void validateColumnNames(List<String> names) throws DDFException {
    Set<String> columnSet = new HashSet<String>();
    if(names != null) {
      for(String name: names) {
        if(columnSet.contains(name)) {
          throw new DDFException(String.format("Duplicated column name %s", name));
        } else {
          if(!Utils.isAlphaNumeric(name)) {
            throw new DDFException(String.format("Invalid column name %s, only allow alphanumeric (uppercase and lowercase a-z, numbers 0-9) " +
                "and dash (\"-\") and underscore (\"_\")", name));
          }
          columnSet.add(name);
        }
      }
    } else {
      throw new DDFException("names is null");
    }
  }

  private List<Column> parseColumnList(String columnList) {
    if (Strings.isNullOrEmpty(columnList))
      return null;
    String[] segments = columnList.split("\\s*,\\s*");

    mColumns.clear();
    for (String segment : segments) {
      if (Strings.isNullOrEmpty(segment))
        continue;

      String[] parts = segment.split("\\s+");
      if (Strings.isNullOrEmpty(parts[0])
          || Strings.isNullOrEmpty(parts[1]))
        continue;

      mColumns.add(new Column(parts[0], parts[1]));
    }

    return mColumns;
  }

  public String getTableName() {
    return mTableName;
  }

  public void setTableName(String mTableName) {
    this.mTableName = mTableName;
  }

  public List<Column> getColumns() {
    List<Column> columns = new ArrayList<Column>();
    for (Column column : mColumns) {
      columns.add(column.clone());
    }
    return columns;
  }

  public void setColumns(List<Column> columns) throws DDFException {
    this.mColumns = columns;
  }

  public List<String> getColumnNames() {
    List<String> columnNames = Lists.newArrayList();
    for (Column col : mColumns) {
      columnNames.add(col.getName());
    }
    return columnNames;
  }

  public void setColumnNames(List<String> names) throws DDFException {
    validateColumnNames(names);
    int length = names.size() < mColumns.size() ? names.size() : mColumns
        .size();
    for (int i = 0; i < length; i++) {
      mColumns.get(i).setName(names.get(i));
    }
  }

  public Column getColumn(int i) throws DDFException {
    if (mColumns.isEmpty()) {
      throw new DDFException("List of columns is empty");
    }
    if(i < 0) {
      throw new DDFException("index must be larger or equal to 0");
    }
    if (i >= mColumns.size()) {
      throw new DDFException(String.format("index must be smaller than number of columns: %s", mColumns.size()));
    }

    return mColumns.get(i);
  }

  public String getColumnName(int i) throws DDFException {
    return getColumn(i).getName();
  }

  public Column getColumn(String name) throws DDFException {
    int i = getColumnIndex(name);
    if (i == -1) {
      return null;
    }

    return getColumn(i);
  }

  public int getColumnIndex(String name) throws DDFException {
    if(mColumns.isEmpty()) {
      throw new DDFException("List of columns is empty");
    }

    if (Strings.isNullOrEmpty(name)) {
      throw new DDFException("ColumName cannot be null or empty");
    }

    for (int i = 0; i < mColumns.size(); i++) {
      if (name.equalsIgnoreCase(mColumns.get(i).getName()))
        return i;
    }

    throw new DDFException(String.format("Can't find column %s", name));
  }

  @Override
  public String toString() {
    Iterable<String> columnSpecs = Iterables.transform(getColumns(), new Function<Column, String>() {
      @Override
      public String apply(Column column) {
        return column.getName() + " " + column.getType();
      }
    });
    return Joiner.on(",").join(columnSpecs);
  }

  /**
   * @return number of columns
   */
  public int getNumColumns() {
    return this.mColumns.size();
  }

  public void addColumn(Column col) {
    this.mColumns.add(col);
  }

  /**
   * Remove a column by its name
   *
   * @param name Column name
   * @return true if succeed
   */
  public void removeColumn(String name) throws DDFException {
    this.mColumns.remove(getColumnIndex(name));
  }

  /**
   * Remove a column by its index
   *
   * @param i Column index
   * @return true if succeed
   */
  public void removeColumn(int i) throws DDFException {
    this.mColumns.remove(i);
  }

  /**
   * This class represents the metadata of a column
   */
  public static class Column implements Serializable {
    @Expose
    private String mName;
    @Expose
    private ColumnType mType;
    @Expose
    private ColumnClass mClass;
    private Factor<?> mOptionalFactor;

    public Column(String name, ColumnType type) {
      this.mName = name;
      this.mType = type;
      this.mClass = ColumnClass.get(type);
    }

    public Column(String name, String type) {
      this(name, ColumnType.get(type));
    }

    public String getName() {
      return mName;
    }

    public Column setName(String name) {
      this.mName = name;
      return this;
    }

    public ColumnType getType() {
      return mType;
    }

    public Column setType(ColumnType type) {
      this.mType = type;
      return this;
    }

    public ColumnClass getColumnClass() {
      return mClass;
    }

    public Column setColumnClass(ColumnClass clazz) {
      this.mClass = clazz;
      return this;
    }

    public boolean isNumeric() {
      return ColumnType.isNumeric(mType);
    }

    /**
     * Sets this column as a {@link Factor}
     *
     * @param factor the {@link Factor} to associate with this column
     * @return
     */
    public <T> Column setAsFactor(Factor<T> factor) {
      mClass = ColumnClass.FACTOR;
      mOptionalFactor = factor;
      return this;
    }

    public Column unsetAsFactor() {
      mClass = ColumnClass.get(mType);
      mOptionalFactor = null;
      return this;
    }

    public Factor<?> getOptionalFactor() {
      return mOptionalFactor;
    }

    @Override
    public Column clone() {
      Column clonedColumn = new Column(this.getName(), this.getType());
      if (mClass == ColumnClass.FACTOR) {
        clonedColumn = clonedColumn.setAsFactor(mOptionalFactor);
      }
      return clonedColumn;
    }
  }


  /**
   *
   */
  public static class ColumnWithData extends Column {
    private Object[] mData;

    public ColumnWithData(String name, Object[] data) {
      super(name, ColumnType.get(data));
    }

    /**
     * @return the data
     */
    public Object[] getData() {
      return mData;
    }

    /**
     * @param data the data to set
     */
    public void setData(Object[] data) {
      this.mData = data;
    }
  }


  public enum ColumnType {
    TINYINT(Byte.class),
    SMALLINT(Short.class),
    INT(Integer.class),
    BIGINT(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    DECIMAL(java.math.BigDecimal.class),
    STRING(String.class),
    BINARY(Byte[].class),
    BOOLEAN(Boolean.class),
    TIMESTAMP(java.sql.Timestamp.class),
    DATE(java.sql.Date.class),
    ARRAY(scala.collection.Seq.class),
    STRUCT(Objects.class), // TODO review
    MAP(scala.collection.Map.class),
    VECTOR(),
    BLOB(Object.class), //
    ANY(/* for ColumnClass.Factor */) //
    ;


    private List<Class<?>> mClasses = Lists.newArrayList();

    private ColumnType(Class<?>... acceptableClasses) {
      if (acceptableClasses != null && acceptableClasses.length > 0) {
        for (Class<?> cls : acceptableClasses) {
          mClasses.add(cls);
        }
      }
    }

    public List<Class<?>> getClasses() {
      return mClasses;
    }

    public static ColumnType get(String s) {
      if (s == null || s.length() == 0)
        return null;

      for (ColumnType type : values()) {
        if (type.name().equalsIgnoreCase(s))
          return type;

        for (Class<?> cls : type.getClasses()) {
          if (cls.getSimpleName().equalsIgnoreCase(s))
            return type;
        }
      }

      return null;
    }

    public static ColumnType get(Object obj) {
      if (obj != null) {
        Class<?> objClass = obj.getClass();

        for (ColumnType type : ColumnType.values()) {
          for (Class<?> cls : type.getClasses()) {
            if (cls.isAssignableFrom(objClass))
              return type;
          }
        }
      }

      return null;
    }

    public static ColumnType get(Object[] elements) {
      return (elements == null || elements.length == 0 ? null
          : get(elements[0]));
    }

    public static boolean isNumeric(ColumnType colType) {
      switch (colType) {
        case TINYINT:
        case SMALLINT:
        case INT:
        case BIGINT:
        case DOUBLE:
        case FLOAT:
        case DECIMAL:
        case VECTOR:
          return true;

        default:
          return false;
      }
    }

    public static boolean isIntegral(ColumnType colType) {
      switch (colType) {
        case TINYINT:
        case SMALLINT:
        case INT:
        case BIGINT:
         return true;

        default:
          return false;
      }
    }

    public static boolean isFractional(ColumnType colType) {
      switch (colType) {
        case DOUBLE:
        case FLOAT:
        case DECIMAL:
          return true;

        default:
          return false;
      }
    }
  }


  /**
   * The R concept of a column class.
   * // TODO review and update @huan @freeman @nhanitvn
   */
  public enum ColumnClass {
    NUMERIC(ColumnType.TINYINT, ColumnType.SMALLINT, ColumnType.INT, ColumnType.BIGINT, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.DECIMAL), //
    CHARACTER(ColumnType.STRING), //
    LOGICAL(ColumnType.BOOLEAN), //
    FACTOR(ColumnType.ANY) // ??
    ;

    private List<ColumnType> mTypes = Lists.newArrayList();

    private ColumnClass(ColumnType... acceptableColumnTypes) {
      if (acceptableColumnTypes != null
          && acceptableColumnTypes.length > 0) {
        for (ColumnType type : acceptableColumnTypes) {
          mTypes.add(type);
        }
      }
    }

    public List<ColumnType> getColumnTypes() {
      return mTypes;
    }

    /**
     * Returns the appropriate {@link ColumnClass} for a given
     * {@link ColumnType}
     *
     * @param type
     * @return
     */
    public static ColumnClass get(ColumnType type) {
      if (type == null)
        return null;

      for (ColumnClass clz : ColumnClass.values()) {
        for (ColumnType t : clz.getColumnTypes()) {
          if (type.equals(t))
            return clz;
        }
      }

      return null;
    }
  }

  public static class DummyCoding implements Serializable {
    private Map<Integer, Map<String, java.lang.Double>> mapping = new HashMap<Integer, Map<String, java.lang.Double>>();
    private Map<String, Map<String, java.lang.Double>> mColNameMapping = null;
    private Integer numDummyCoding;
    public int[] xCols;
    private Integer numberFeatures = 0;

    public Map<Integer, Map<String, java.lang.Double>> getMapping() {
      return mapping;
    }

    public void setMapping(Map<Integer, Map<String, java.lang.Double>> mapping) {
      this.mapping = mapping;
    }
    
    public void setColNameMapping(Map<String, Map<String, java.lang.Double>> colMapping) {
       this.mColNameMapping = colMapping;
    }

    public Map<String, Map<String, java.lang.Double>> getColNameMapping() {
      return this.mColNameMapping;
    }
    public Integer getNumDummyCoding() {
      return numDummyCoding;
    }

    public void setNumDummyCoding(Integer numDummyCoding) {
      this.numDummyCoding = numDummyCoding;
    }

    public int[] getxCols() {
      return xCols;
    }

    public void setxCols(int[] xCols) {
      this.xCols = xCols;
    }

    public Integer getNumberFeatures() {
      return numberFeatures;
    }

    public void setNumberFeatures(Integer numberFeatures) {
      this.numberFeatures = numberFeatures;
    }

  }
}
