/**
 *
 */
package io.spark.ddf.content;


import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.Factor;
import io.ddf.content.IHandleRepresentations;
import io.ddf.content.Schema;
import io.ddf.content.Schema.Column;
import io.ddf.content.Schema.ColumnType;
import io.ddf.exception.DDFException;
import io.spark.ddf.SparkDDF;
import io.spark.ddf.util.SparkUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class SchemaHandler extends io.ddf.content.SchemaHandler {

  public SchemaHandler(DDF theDDF) {
    super(theDDF);
  }

  public static Schema getSchemaFromDataFrame(DataFrame rdd) {
    return SparkUtils.schemaFromDataFrame(rdd);
  }

  @Override
  public void setFactorLevelsForStringColumns(String[] xCols) throws DDFException {
    for (int i = 0; i < xCols.length; i++) {
      Column c = this.getColumn(xCols[i]);
      if (c.getType() == ColumnType.STRING) {
        this.setAsFactor(c.getName());
      }
    }
  }


  @Override
  public void computeFactorLevelsAndLevelCounts() throws DDFException {
    List<Integer> columnIndexes = new ArrayList<Integer>();
    List<Schema.ColumnType> columnTypes = new ArrayList<Schema.ColumnType>();

    for (Column col : this.getColumns()) {
      if (col.getColumnClass() == Schema.ColumnClass.FACTOR) {
        Factor<?> colFactor = col.getOptionalFactor();

        if (colFactor == null || colFactor.getLevelCounts() == null || colFactor.getLevels() == null) {
          if (colFactor == null) {
            colFactor = this.setAsFactor(col.getName());
          }
          columnIndexes.add(this.getColumnIndex(col.getName()));
          columnTypes.add(col.getType());
          //factors.add(colFactor);
        }
      }
    }

    Map<Integer, Map<String, Integer>> listLevelCounts;
    IHandleRepresentations repHandler = this.getDDF().getRepresentationHandler();
    if (columnIndexes.size() > 0) {
      try {
        if (repHandler.has(RDD.class, Object[].class)) {
          RDD<Object[]> rdd = ((SparkDDF) this.getDDF()).getRDD(Object[].class);
          listLevelCounts = GetMultiFactor.getFactorCounts(rdd,
              columnIndexes, columnTypes, Object[].class);
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
        if(levelCounts != null) {
          Factor<?> factor = column.getOptionalFactor();
          List<String> levels = new ArrayList<String>(levelCounts.keySet());
          factor.setLevelCounts(levelCounts);
          factor.setLevels(levels, false);
        }
      }
    }
  }
}

