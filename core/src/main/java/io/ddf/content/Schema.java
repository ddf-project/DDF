package io.ddf.content;


import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.Expose;
import io.ddf.Factor;
import io.ddf.exception.DDFException;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;


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
    Set<String> columnSet = new HashSet<String>();
    for(Column column: columns) {
      if(columnSet.contains(column.getName())) {
        throw new DDFException(String.format("Duplicated column name %s", column.getName()));
      } else {
        columnSet.add(column.getName());
      }
    }
    this.mTableName = tableName;
    this.mColumns = columns;
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

  public void setColumns(List<Column> Columns) {
    this.mColumns = Columns;
  }

  public List<String> getColumnNames() {
    List<String> columnNames = Lists.newArrayList();
    for (Column col : mColumns) {
      columnNames.add(col.getName());
    }
    return columnNames;
  }

  public void setColumnNames(List<String> names) {
    int length = names.size() < mColumns.size() ? names.size() : mColumns
        .size();
    for (int i = 0; i < length; i++) {
      mColumns.get(i).setName(names.get(i));
    }
  }

  public Column getColumn(int i) {
    if (mColumns.isEmpty()) {
      return null;
    }
    if (i < 0 || i >= mColumns.size()) {
      return null;
    }

    return mColumns.get(i);
  }

  public String getColumnName(int i) {
    if (getColumn(i) == null) {
      return null;
    }
    return getColumn(i).getName();
  }

  public Column getColumn(String name) {
    int i = getColumnIndex(name);
    if (i == -1) {
      return null;
    }

    return getColumn(i);
  }

  public int getColumnIndex(String name) {
    if (mColumns.isEmpty() || Strings.isNullOrEmpty(name))
      return -1;

    for (int i = 0; i < mColumns.size(); i++) {
      if (name.equalsIgnoreCase(mColumns.get(i).getName()))
        return i;
    }

    return -1;
  }

  /*
   *
   */
  public void generateDummyCoding() throws NumberFormatException,
      DDFException {
    DummyCoding dc = new DummyCoding();
    // initialize array xCols which is just 0, 1, 2 ..
    dc.xCols = new int[this.getColumns().size()];
    int i = 0;
    while (i < dc.xCols.length) {
      dc.xCols[i] = i;
      i += 1;
    }

    List<Column> columns = this.getColumns();
    Iterator<Column> it = columns.iterator();
    int count = 0;
    while (it.hasNext()) {
      Column currentColumn = it.next();
      int currentColumnIndex = this.getColumnIndex(currentColumn.getName());
      HashMap<String, java.lang.Double> temp = new HashMap<String, java.lang.Double>();
      // loop
      if (currentColumn.getColumnClass() == ColumnClass.FACTOR) {
        //set as factor
        //recompute level
        List<String> levels = new ArrayList(currentColumn.getOptionalFactor().getLevels());
        currentColumn.getOptionalFactor().setLevels(levels, true);

        Map<String, Integer> currentColumnFactor = currentColumn.getOptionalFactor().getLevelMap();
        Iterator<String> iterator = currentColumnFactor.keySet()
            .iterator();

        //TODO update this code
        i = 0;
        temp = new HashMap<String, java.lang.Double>();
        while (iterator.hasNext()) {
          String columnValue = iterator.next();
          temp.put(columnValue, Double.parseDouble(i + ""));
          i += 1;
        }
        dc.getMapping().put(currentColumnIndex, temp);
        count += temp.size() - 1;
      }
    }
    dc.setNumDummyCoding(count);

    // TODO hardcode remove this
    // HashMap<String, Double> temp2 = new HashMap<String, Double>();
    // temp2.put("IAD", 1.0);
    // temp2.put("IND", 2.0);
    // temp2.put("ISP", 3.0);
    // dc.getMapping().put(1, temp2);
    // dc.setNumDummyCoding(2);

    // ignore Y column
    Integer _features = this.getNumColumns() - 1;
    // plus bias term for linear model
    _features += 1;
    // plus the new dummy coding columns
    _features += dc.getNumDummyCoding();

    //dc.getMapping().size() means number of factor column
    _features -= (!dc.getMapping().isEmpty()) ? dc.getMapping().size() : 0;
    dc.setNumberFeatures(_features);
    // set number of features in schema

    this.setDummyCoding(dc);

  }

  public DummyCoding getDummyCoding() {
    return dummyCoding;
  }

  public void setDummyCoding(DummyCoding dummyCoding) {
    dummyCoding.toPrint();
    this.dummyCoding = dummyCoding;
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
  public boolean removeColumn(String name) {
    if (getColumnIndex(name) < 0)
      return false;
    this.mColumns.remove(getColumnIndex(name));
    return true;
  }

  /**
   * Remove a column by its index
   *
   * @param i Column index
   * @return true if succeed
   */
  public boolean removeColumn(int i) {
    if (getColumn(i) == null)
      return false;
    this.mColumns.remove(i);
    return true;
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
    STRUCT(Objects.class),
    //STRUCT(org.apache.spark.sql.Row.class), // TODO review
    MAP(scala.collection.Map.class),
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
    private HashMap<Integer, HashMap<String, java.lang.Double>> mapping = new HashMap<Integer, HashMap<String, java.lang.Double>>();
    private Map<String, Map<String, java.lang.Double>> mColNameMapping = null;
    private Integer numDummyCoding;
    public int[] xCols;
    private Integer numberFeatures = 0;

    public void toPrint() {

      Iterator it = mapping.keySet().iterator();
      while (it.hasNext()) {
        HashMap<String, Double> a = getMapping().get(it.next());
        Iterator<String> b = a.keySet().iterator();
      }
    }

    public HashMap<Integer, HashMap<String, java.lang.Double>> getMapping() {
      return mapping;
    }

    public void setMapping(HashMap<Integer, HashMap<String, java.lang.Double>> mapping) {
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
