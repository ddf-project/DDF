package io.ddf.util;

import io.ddf.exception.DDFException;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by jing on 4/14/16.
 */
public class UtilsTests {
  @Test
  public void testSanitizeColumnNames() throws DDFException {
    final List<ImmutableList<String>> namesList = new ArrayList<>();
    namesList.add(new ImmutableList.Builder<String>()
        .add("dd2_2") // valid
        .add("_dd2_2") // start with dash
        .add("2dd2_2") // start with num
        .add("@dd2_2") // start with others
        .add("dd 22") // space
        .add("dd 22 ") // back space
        .build());

    final List<ImmutableList<String>> expectedList = new ArrayList<>();
    expectedList.add(new ImmutableList.Builder<String>()
        .add("dd2_2")
        .add("dd2_2_0")
        .add("dd2_2_1")
        .add("dd2_2_2")
        .add("dd_22")
        .add("dd_22_0")
        .build());

    for (int i = 0; i < namesList.size(); ++i) {
      List names = namesList.get(i);
      assert (Utils.sanitizeColumnName(names).equals(expectedList.get(i)));
    }
  }
}
