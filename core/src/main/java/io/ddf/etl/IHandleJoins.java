package io.ddf.etl;


import java.util.List;
import io.ddf.DDF;
import io.ddf.etl.Types.JoinType;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleJoins extends IHandleDDFFunctionalGroup {

  public DDF join(DDF anotherDDF, JoinType joinType, List<String> byColumns, List<String> byLeftColumns,
      List<String> byRightColumns) throws DDFException;
}
