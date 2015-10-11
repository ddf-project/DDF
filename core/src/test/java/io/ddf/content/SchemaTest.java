package io.ddf.content;


import io.ddf.exception.DDFException;
import org.junit.Test;

import java.util.HashMap;

public class SchemaTest {
  class Panda {
    int legs;
    String name;

    public void setName(String name) {
      this.name = name;
    }

    public Panda(int legs, String name) {
      this.legs = legs;
      this.name = name;
    }

    @Override
    public String toString() {
      return String.format("%s has %s legs", name, legs);
    }
  }

  @Test
  public void testCreateSchema() {
    HashMap<Integer, Panda> m = new HashMap<Integer, Panda>();
    Panda po = new Panda(4, "Po");
    m.put(1, po);
    m.put(2, po);
    System.out.println(m.get(1).toString());
    System.out.println(m.get(2).toString());
    po.setName("Mei");
    System.out.println(m.get(1).toString());
    System.out.println(m.get(2).toString());
  }

  @Test
  public void testParseSchema() throws DDFException {
    String columns = "row Int, \n\tprice double, \n\t lotsize int, \n\t bedrooms int,\n\tbathrms int";
    Schema schema = new Schema(null, columns);
    for(Schema.Column column: schema.getColumns()) {
      assert(column.getName() != null);
      assert(column.getType().toString() != null);
    }
  }

  @Test(expected = DDFException.class)
  public void testDuplicatedColumn() throws DDFException {
    String columns = "row Int, \n\tprice double, \n\t lotsize int, \n\t bedrooms int,\n\tbathrms int, row Int";
    Schema schema = new Schema(null, columns);

  }
}
