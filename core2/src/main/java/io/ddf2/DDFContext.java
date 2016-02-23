package io.ddf2;


import java.util.HashMap;
import java.util.Map;

/**
 * Help to share data across classes.
 */
public class DDFContext {

    protected static final ThreadLocal<Property> context = ThreadLocal.withInitial(() ->  { return new Property();});


    public static void setProperty(String key, Object value) {
        context.get().setProperty(key,value);
    }

    public static Object getProperty(String key) {
        try{
            return context.get().getProperty(key);
        }catch (Exception ex){
            return null;
        }
    }

    static class Property {
        protected Map<String, Object> properties = new HashMap<>();

        public void setProperty(String key, Object value) {
            properties.put(key, value);

        }

        public Object getProperty(String key) {
            return properties.get(key);
        }
    }
}
 
