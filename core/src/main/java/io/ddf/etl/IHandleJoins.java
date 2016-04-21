package io.ddf.etl;


import io.ddf.DDF;
import io.ddf.etl.Types.JoinType;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.List;

public interface IHandleJoins extends IHandleDDFFunctionalGroup {

  public DDF join(DDF anotherDDF, JoinType joinType, List<String> byColumns, List<String> byLeftColumns,
                  List<String> byRightColumns) throws DDFException;

  public DDF join(DDF anotherDDF, JoinType joinType, List<String> byColumns, List<String> byLeftColumns,
                  List<String> byRightColumns, String leftSuffix, String rightSuffix) throws DDFException;

  public DDF merge(DDF anotherDDF) throws DDFException;
}
