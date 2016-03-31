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
import io.ddf.spark.SparkDDFManager;
import io.ddf.spark.analytics.FactorIndexer;
import io.ddf.spark.util.SparkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import scala.Tuple2;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SchemaHandler extends io.ddf.content.SchemaHandler {
    /**
     * This default constructor is only used for Serializable
     */
    protected SchemaHandler() {
    }

    public SchemaHandler(DDF theDDF) {
        super(theDDF);
    }

    public static Schema getSchemaFromDataFrame(DataFrame rdd) {
        return SparkUtils.schemaFromDataFrame(rdd);
    }

    /**
     * @param df       spark dataframe that may contain struct columns
     * @param colNames list of columns that need to be flattened.
     * @return a list of non-struct columns flattened from columns in colNames. If colNames is empty or null, then return
     * a list of flattened column names from the entire input dataframe, i.e. from all the columns.
     */
    public static String[] getFlattenedColumnsFromDataFrame(DataFrame df, String[] colNames) {
        return SparkUtils.flattenColumnNamesFromDataFrame(df, colNames);
    }

    public static String[] getFlattenedColumnsFromDataFrame(DataFrame rdd) {
        return getFlattenedColumnsFromDataFrame(rdd, null);
    }

    @Override
    public Map<String, Map<String, Integer>> computeLevelCounts(String[] columnNames) throws DDFException {
        if (columnNames.length > 0) {
            List<Integer> columnIndexes = new ArrayList<Integer>();
            List<Schema.ColumnType> columnTypes = new ArrayList<Schema.ColumnType>();

            for (String columnName : columnNames) {
                this.setAsFactor(columnName);
                columnIndexes.add(this.getColumnIndex(columnName));
                columnTypes.add(this.getColumn(columnName).getType());
            }

            Map<Integer, Map<String, Integer>> listLevelCounts;

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
            Map<String, Map<String, Integer>> listLevelCountsWithName = new HashMap<String, Map<String, Integer>>();

            for (Integer columnIndex : columnIndexes) {
                String colName = this.getDDF().getColumnName(columnIndex);
                listLevelCountsWithName.put(colName, listLevelCounts.get(columnIndex));
            }
            return listLevelCountsWithName;
        } else {
            return new HashMap<String, Map<String, Integer>>();
        }
    }

    @Override
    public List<Object> computeFactorLevels(String columnName) throws DDFException {
        DataFrame df = (DataFrame) this.getDDF().getRepresentationHandler().get(DataFrame.class);
        DataFrame distinctDF = df.select(columnName).distinct();
        Long distinctCount = distinctDF.count();
        if (distinctCount > Factor.getMaxLevelCounts()) {
            throw new DDFException(String
                    .format("Number of distinct values in column %s is %s larger than MAX_LEVELS_COUNTS = %s", columnName,
                            distinctCount, Factor.getMaxLevelCounts()));
        }

        List<Object> listValues = FactorIndexer.getFactorMapForColumn(this.getDDF(), columnName).values();
        return listValues;
    }

    /**
     * applySchema(Schema newSchema) will do apply change column type from current to new data type.
     * This function return new DDF and also statistic when its applied new schema
     *
     * @param schema    will use to apply for current DDF
     * @param isDropRow flag is true, we will drop row which contains any column couldn't parse to new type
     * @return Tuple2<SparkDDF,ApplySchemaStatistic>
     * +SparkDDF: new sparkddf which applied new schema already
     * +ApplySchemaStatistic:  applied schema statistic @see ApplySchemaStatistic
     */
    public Tuple2<SparkDDF, ApplySchemaStatistic> applySchema(Schema schema, boolean isDropRow) throws DDFException {
        long startApplySchema = System.currentTimeMillis();
        // Keep info to Convert column at <pos> to <ColumnType>
        List<Tuple2<Integer, Column>> listConvertColumns = getListConvertColumn(this.getSchema(), schema);

        try {
            SparkDDFManager manager = (SparkDDFManager) this.getManager();
//      SparkContext sparkContext = manager.getSparkContext();
            Accumulator<ApplySchemaStatistic> statisticAccumulator = manager.getSparkContext().accumulator(new ApplySchemaStatistic(), ApplySchemaStatisticParam.getInstance());

            JavaRDD<Row> rdd = ((SparkDDF) this.getDDF()).getRDD(Row.class).toJavaRDD();


            //JavaRDD<Row> appliedRdd = rdd.filter(row -> convertRowType(row, listConvertColumns, statisticAccumulator));
            JavaRDD<Row> appliedRdd = rdd.map(row -> convertRowType(row, listConvertColumns, statisticAccumulator, isDropRow));
            if (isDropRow) appliedRdd = appliedRdd.filter(row -> {
                return row != null;
            });
            long totalLineSuccess = appliedRdd.count();
            ApplySchemaStatistic statistic = statisticAccumulator.value();
            statistic.setTotalLineSuccessed(totalLineSuccess);
            if (mLog.isDebugEnabled()) {
                mLog.debug("ApplySchema in " + (System.currentTimeMillis() - startApplySchema) + " ms");
                mLog.debug("Total Line Processed: " + statistic.getTotalLineProcessed());
                mLog.debug("Total Line Succeed: " + statistic.getTotalLineSuccessed());
                if (mLog.isTraceEnabled()) {
                    statistic.getMapColumnStatistic().forEach((name, columnStatistic) -> {
                        mLog.trace("column: " + name + " fail in: " + columnStatistic.getNumConvertedFailed());
                        mLog.trace("sample: " + StringUtils.join(columnStatistic.getSampleData(), ','));
                    });
                }
            }
            if (statistic.getTotalLineSuccessed() > 0) {
                DataFrame dataFrame = manager.getHiveContext().createDataFrame(appliedRdd.rdd(), appliedRdd.first().schema());
                return new Tuple2<>((SparkDDF) SparkUtils.df2ddf(dataFrame, manager), statistic);
            } else {
                return new Tuple2<>(null, statistic);
            }
        } catch (ClassCastException cce) {
            mLog.error("Exception when down cast from DDFManager to SparkDDFManager", cce);
            throw new DDFException("applySchema only works with SparkDDFManager");
        }

    }

    /**
     * Convert Row Data To Given Type
     *
     * @param row                  which hold multiple columns to convert
     * @param listConvertColumns   given columnIndex & columnName &  type to convert to
     * @param statisticAccumulator statistic accumulator
     */
    private Row convertRowType(Row row, List<Tuple2<Integer, Column>> listConvertColumns,
                               Accumulator<ApplySchemaStatistic> statisticAccumulator, boolean isDropRow) {
        statisticAccumulator.localValue().increaseLineProcessed();

        Object[] rowData = toArray(row);
        boolean isFail = false;
        for (Tuple2<Integer, Column> convertColumn : listConvertColumns) {
            int colIndex = convertColumn._1();
            Schema.ColumnType toType = convertColumn._2().getType();
            Object colValue = rowData[colIndex];
            if (colValue != null) {
                String columnData = colValue.toString().trim();
                Object newColData = cast(columnData, toType);
                if (newColData == null) {
                    statisticAccumulator.localValue().addColumnFailed(convertColumn._2().getName(), columnData);
                    isFail = true;
                }
                rowData[colIndex] = newColData;
            }
        }
        if (isFail && isDropRow) {
            return null;
        } else {
            return new GenericRow(rowData);
        }
    }

    private Object[] toArray(Row row) {
        Object[] values = new Object[row.size()];
        for (int i = 0; i < row.size(); ++i) {
            values[i] = row.get(i);
        }
        return values;
    }

    /**
     * Check whether we could convert data at row[index] requiredType @see Schema.ColumnType
     *
     * @param data:         Data to convert to requiredType
     * @param requiredType: type we need to convert to
     * @return newData in required type, null if couldn't convert
     */
    private Object cast(String data, Schema.ColumnType requiredType) {
        try {
            switch (requiredType) {
                case TINYINT:
                    return Byte.valueOf(data) != null;
                case SMALLINT:
                    return Short.valueOf(data) != null;
                case INT:
                    return Integer.valueOf(data) != null;
                case BIGINT:
                    return Long.valueOf(data) != null;
                case FLOAT:
                    return Float.valueOf(data) != null;
                case DOUBLE:
                    return Double.valueOf(data) != null;
                case DECIMAL:
                    return new BigDecimal(data) != null;
                case STRING:
                    return true;
                case BINARY:
                    return data.getBytes() != null;
                case BOOLEAN:
                    return Boolean.valueOf(data);
                case TIMESTAMP:
                    return Timestamp.valueOf(data) != null;
                case DATE:
                    return Date.valueOf(data) != null;
                case ARRAY:
                case ANY:
                case BLOB:
                    return true;
                case STRUCT:
                case MAP:
                    return false; //need more info to parse
            }
        } catch (ClassCastException | NullPointerException | IllegalArgumentException nfe) {
            mLog.debug(nfe.getMessage());
        }
        return false;
    }

    /**
     * get list column need to be convert
     *
     * @return List<Tuple3<Integer,String,ColumnType>> = ColumnIndex,ColumnName,ColumnType
     */
    protected List<Tuple2<Integer, Column>> getListConvertColumn(Schema fromSchema, Schema toSchema) throws DDFException {
        List<Tuple2<Integer, Column>> listConvertColumn = new ArrayList<>();
        Map<String, Integer> mapNameIndex = new HashMap<>();
        List<String> columnNames = fromSchema.getColumnNames();
        int i = 0;
        for (String name : columnNames) {
            mapNameIndex.put(name, i++);
        }
        for (Column column : toSchema.getColumns()) {
            if (!mapNameIndex.containsKey(column.getName()))
                throw new DDFException("Unknown column: " + column.getName());
            listConvertColumn.add(new Tuple2(mapNameIndex.get(column.getName()), column));
        }
        return listConvertColumn;
    }

    /**
     * ApplySchemaStatistic will hold: + total line processed + total line success + column failed parsed info (total
     * failed on this column & example )
     */
    @NotThreadSafe
    public static class ApplySchemaStatistic implements Serializable {
        private final int NUM_EXAMPLE_DATA = 10;
        private final HashMap</*Column Name*/String, ColumnStatistic> mapColumnStatistic = new HashMap();
        private Long totalLineProcessed = 0L;
        private Long totalLineSuccessed = 0L;

        public static ApplySchemaStatistic add(ApplySchemaStatistic s1, ApplySchemaStatistic s2) {
            ApplySchemaStatistic statistic = new ApplySchemaStatistic();
            statistic.totalLineProcessed = s1.totalLineProcessed + s2.totalLineProcessed;
            s1.mapColumnStatistic.forEach((colName, columnStatistic) -> {
                for (String data : columnStatistic.getSampleData()) statistic.addColumnFailed(colName, data);
            });
            s2.mapColumnStatistic.forEach((colName, columnStatistic) -> {
                for (String data : columnStatistic.getSampleData()) statistic.addColumnFailed(colName, data);
            });
            return statistic;
        }

        public void increaseLineProcessed() {
            ++totalLineProcessed;
        }

        public void addColumnFailed(String colName, String data) {
            if (!mapColumnStatistic.containsKey(colName)) {
                mapColumnStatistic.put(colName, new ColumnStatistic());
            }
            ColumnStatistic statistic = mapColumnStatistic.get(colName);
            statistic.increaseNumFailed();
            if (statistic.getNumSampleData() < NUM_EXAMPLE_DATA) {
                statistic.addSample(data);
            }
        }

        public Long getTotalLineProcessed() {
            return totalLineProcessed;
        }

        public Long getTotalLineSuccessed() {
            return totalLineSuccessed;
        }

        public void setTotalLineSuccessed(Long totalLineSuccessed) {
            this.totalLineSuccessed = totalLineSuccessed;
        }

        public Map<String, ColumnStatistic> getMapColumnStatistic() {
            return mapColumnStatistic;
        }

        public class ColumnStatistic implements Serializable {
            private final List<String> listSampleData = new ArrayList<>();
            private Long numConvertedFailed = 0L;

            public void addSample(String sample) {
                listSampleData.add(sample);
            }

            public void addNumFailed(int numFailed) {
                numConvertedFailed += numFailed;
            }

            public void increaseNumFailed() {
                ++numConvertedFailed;
            }

            public List<String> getSampleData() {
                return listSampleData;
            }

            public int getNumSampleData() {
                return listSampleData.size();
            }

            public Long getNumConvertedFailed() {
                return numConvertedFailed;
            }
        }

    }

    static class ApplySchemaStatisticParam implements AccumulatorParam<ApplySchemaStatistic> {
        private final static ApplySchemaStatisticParam inst = new ApplySchemaStatisticParam();

        private ApplySchemaStatisticParam() {
        }

        public static ApplySchemaStatisticParam getInstance() {
            return inst;
        }

        @Override
        public ApplySchemaStatistic addAccumulator(ApplySchemaStatistic t1, ApplySchemaStatistic t2) {
            return merge(t1, t2);
        }

        @Override
        public ApplySchemaStatistic addInPlace(ApplySchemaStatistic r1, ApplySchemaStatistic r2) {
            return merge(r1, r2);
        }

        @Override
        public ApplySchemaStatistic zero(ApplySchemaStatistic initialValue) {
            return new ApplySchemaStatistic();
        }

        private ApplySchemaStatistic merge(ApplySchemaStatistic a1, ApplySchemaStatistic a2) {
            return ApplySchemaStatistic.add(a1, a2);
        }
    }
}

