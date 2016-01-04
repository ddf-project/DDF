package io.ddf2.datasource.fileformat.resolver;

import java.sql.Timestamp;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sangdn on 1/4/16.
 *
 */
public class TypeResolver {
    protected static Map<String,Class> ReserveType = new HashMap<>();
    static{
        ReserveType.put("int", Integer.class);
        ReserveType.put("long", Long.class);
        ReserveType.put("float", Float.class);
        ReserveType.put("double", Double.class);
        ReserveType.put("string", String.class);
        ReserveType.put("boolean", Boolean.class);
        ReserveType.put("bool", Boolean.class);
    }

    /**
     *
     * @param data sample data to resolve to DataType
     * @return
     */
    public static Class resolver(String data) throws UnsupportedTypeException {

        if(data.contains(".")){
            try{
                Double.parseDouble(data);
                return getType("double");
            }catch (NumberFormatException nfe){}
        }
        try {
            Long.parseLong(data);
            return getType("long");
        }catch (NumberFormatException nfe){}
        try{
            Boolean.parseBoolean(data);
            return  getType("bool");
        }catch (NumberFormatException nfe){}

        return getType("string");
    }

    public static Class getType(String type) throws UnsupportedTypeException {
        type = type.toLowerCase();
        if(ReserveType.containsKey(type)) {
            return ReserveType.get(type.toLowerCase());
        }else{
            throw new UnsupportedTypeException("Unsupported DataType: " + type);
        }

    }

}
