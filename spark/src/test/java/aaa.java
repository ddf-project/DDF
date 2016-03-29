import io.ddf.spark.BaseTest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by sangdn on 26/03/2016.
 */
public class aaa {

  public static void main(String[] args){
    Logger logger = LoggerFactory.getLogger(aaa.class);
    logger.debug("debug");
    logger.info("info");
    logger.error("error");
    int i =0;
    Map<Integer,String> test = new HashMap<>();
    for(Integer a : Arrays.asList(1,2,3,4,5)){
      test.put(i++,a + "");
    }
    Iterator<Map.Entry<Integer, String>> iterator = test.entrySet().iterator();
    while(iterator.hasNext()){
      Map.Entry<Integer, String> next = iterator.next();
      System.out.println(next.getKey() + " " + next.getValue());
    }

  }
}
