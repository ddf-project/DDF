package io.ddf2.spark.preparer;

import io.ddf2.datasource.fileformat.FileFormatResolverException;
import io.ddf2.datasource.fileformat.IFileFormatResolver;
import io.ddf2.datasource.schema.Column;
import io.ddf2.datasource.schema.IColumn;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.datasource.schema.Schema;

import java.util.*;

/**
 * Created by sangdn on 1/4/16.
 * Resolve a sample data to get its schema
 * Currently support basic data type:
 */
public class BasicTextFileResolver implements IFileFormatResolver {
    @Override
    public  ISchema resolve(List<String> preferColumnName, List<List<String>> sampleRows) throws Exception {
        //For every colulmn, we detect how many kind of data type exist in sampleRows.
        //Map <"Column-Index", "Map< Type, NumCounterDetected>">
        Map<String, Map<Class, Integer>> mapDataTypeCounter = new HashMap<>();
        int maximumColumn = 0;
        for (List<String> row : sampleRows) {
            if (row.size() > maximumColumn) maximumColumn = row.size();
            for (int colIndex = 0; colIndex < row.size(); ++colIndex) {
                try {
                    Class type = resolver(row.get(colIndex));
                    String colKey = "col_" + colIndex;
                    Map<Class, Integer> mapClassCounter = mapDataTypeCounter.get(colKey);
                    if (mapClassCounter == null) {
                        mapClassCounter = new HashMap<>();
                        mapDataTypeCounter.put(colKey, mapClassCounter);
                    }
                    Integer counter = 0;
                    if (mapClassCounter.containsKey(type)) {
                        counter += mapClassCounter.get(type);
                    }
                    mapClassCounter.put(type, ++counter);

                } catch (Exception e) {

                }
            }
        }

        Schema schema = new Schema();
        for (int colIndex = 0; colIndex < maximumColumn; ++colIndex) {
            String colKey = "col_" + colIndex;
            String colName;
            if (preferColumnName == null || colIndex >= preferColumnName.size()) {
                colName = "col_" + colIndex;
            } else {
                colName = preferColumnName.get(colIndex);
            }
            Map<Class, Integer> mapTypeCounter = mapDataTypeCounter.get(colKey);
            if (mapTypeCounter == null || mapTypeCounter.size() < 1) {
                throw new FileFormatResolverException("Couldn't Detect Column Type Index=" + colIndex);
            }
            Class colType = null;
            int maximumCounter = -1;
            Iterator<Class> classIterator = mapTypeCounter.keySet().iterator();
            while (classIterator.hasNext()) {
                Class clsType = classIterator.next();
                int counter = mapTypeCounter.get(clsType);
                if (counter > maximumCounter) {
                    colType = clsType;
                }

            }

            schema.append(new Column(colName, colType));

        }

        return schema;
    }

    /**
     * @param data sample data to resolve to DataType
     * @return
     */
    public static Class resolver(String data) throws Exception {

        if (data.contains(".")) {
            try {
                Double.parseDouble(data);
                return Double.class;
            } catch (NumberFormatException nfe) {
            }
        }
        try {
            Long.parseLong(data);
            return Long.class;
        } catch (NumberFormatException nfe) {
        }
        try {
            if (data.equalsIgnoreCase("true") || data.equalsIgnoreCase("false"))
                return Boolean.class;
        } catch (NumberFormatException nfe) {
        }

        return String.class;
    }


}
