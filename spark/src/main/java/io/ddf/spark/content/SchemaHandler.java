/**
 *
 */
package io.ddf.spark.content;


import io.ddf.DDF;
import io.ddf.Factor;
import io.ddf.content.IHandleRepresentations;
import io.ddf.content.Schema;
import io.ddf.content.Schema.Column;
import io.ddf.exception.DDFException;
import io.ddf.spark.SparkDDF;
import io.ddf.spark.util.SparkUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SchemaHandler extends io.ddf.content.SchemaHandler {

  public SchemaHandler(DDF theDDF) {
    super(theDDF);
  }

  public static Schema getSchemaFromDataFrame(DataFrame rdd) {
    return SparkUtils.schemaFromDataFrame(rdd);
  }

  /**
   * @param df       spark dataframe that may contain struct columns
   * @param colNames list of columns that need to be flattened.
   * @return a list of non-struct columns flattened from columns in colNames. If colNames is empty or null,
   * then return a list of flattened column names from the entire input dataframe, i.e. from all the columns.
   */
  public static String[] getFlattenedColumnsFromDataFrame(DataFrame df, String[] colNames) {
    return SparkUtils.flattenColumnNamesFromDataFrame(df, colNames);
  }

  public static String[] getFlattenedColumnsFromDataFrame(DataFrame rdd) {
    return getFlattenedColumnsFromDataFrame(rdd, null);
  }

  @Override public Map<String, Map<String, Integer>> computeLevelCounts(String[] columnNames) throws DDFException {
    if (columnNames.length > 0) {
      List<Integer> columnIndexes = new ArrayList<Integer>();
      List<Schema.ColumnType> columnTypes = new ArrayList<Schema.ColumnType>();

      for (String columnName : columnNames) {
        this.setAsFactor(columnName);
        columnIndexes.add(this.getColumnIndex(columnName));
        columnTypes.add(this.getColumn(columnName).getType());
      }

      Map<Integer, Map<String, Integer>> listLevelCounts;
      Map<String, Map<String, Integer>> listLevelCountsWithName = new HashMap<String, Map<String, Integer>>();
      IHandleRepresentations repHandler = this.getDDF().getRepresentationHandler();

      try {
        if (repHandler.has(RDD.class, Object[].class)) {
          RDD<Object[]> rdd = ((SparkDDF) this.getDDF()).getRDD(Object[].class);
          listLevelCounts = GetMultiFactor.getFactorCounts(rdd, columnIndexes, columnTypes, Object[].class);
        } else {
          RDD<Object[]> rdd = ((SparkDDF) this.getDDF()).getRDD(Object[].class);
          if (rdd == null) {
            throw new DDFException("RDD is null");
          }
          listLevelCounts = GetMultiFactor.getFactorCounts(rdd, columnIndexes, columnTypes, Object[].class);
        }
      } catch (DDFException e) {
        throw new DDFException("Error getting factor level counts", e);
      }

      if (listLevelCounts == null) {
        throw new DDFException("Error getting factor levels counts");
      }

      for (Integer colIndex : columnIndexes) {
        Column column = this.getColumn(this.getColumnName(colIndex));
        Map<String, Integer> levelCounts = listLevelCounts.get(colIndex);
        if (levelCounts != null) {
          Factor<?> factor = column.getOptionalFactor();
          List<String> levels = new ArrayList<String>(levelCounts.keySet());
          factor.setLevels(levels, false);
        }
      }
      for (Integer columnIndex : columnIndexes) {
        String colName = this.getDDF().getColumnName(columnIndex);
        listLevelCountsWithName.put(colName, listLevelCounts.get(columnIndex));
      }
      return listLevelCountsWithName;
    } else {
      return new HashMap<String, Map<String, Integer>>();
    }
  }
}

