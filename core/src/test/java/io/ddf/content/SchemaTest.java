package io.ddf.content;


import java.util.HashMap;
import org.junit.Test;

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

}
