package io.ddf2.datasource.fileformat.resolver;

import io.ddf2.datasource.fileformat.IFileFormatResolver;
import io.ddf2.datasource.schema.IColumn;
import io.ddf2.datasource.schema.ISchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sangdn on 1/4/16.
 */
public class TextFileResolver implements IFileFormatResolver {
    @Override
    public ISchema resolve(List<String> preferColumnName, List<List<String>> sampleRows) {
        //Map <"Column-Index", "Map< Type, NumCounterDetected>">
        Map<String, Map<Class,Integer>> mapDataTypeCounter = new HashMap<>();

        for(List<String> row : sampleRows){
            for(int colIndex = 0; colIndex < row.size(); ++colIndex){
                String data = row.get(colIndex);
            }
        }
        return null;
    }
}
