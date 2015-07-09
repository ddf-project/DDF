package io.ddf.etl;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.datasource.DataFormat;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.List;

public interface IHandleSql extends IHandleSqlLike, IHandleDDFFunctionalGroup {
    public SqlResult sqlHandle(String command, Integer maxRows, String dataSource) throws DDFException;
    public SqlResult sqlHandle(String command, Integer maxRows, String dataSource, String namespace) throws DDFException;
    public SqlResult sqlHandle(String command, Integer maxRows, String dataSource, List<String> uriList) throws DDFException;
    //public SqlResult sqlHandle(String command, Integer maxRows, String dataSource, List<DDF> ddfList) throws DDFException;

    public DDF sql2ddfHandle(String command, Schema schema,
                             String dataSource, DataFormat dataFormat) throws DDFException;

    public DDF sql2ddfHandle(String command, Schema schema,
                             String dataSource, DataFormat dataFormat, String namespace) throws DDFException;

    public DDF sql2ddfHandle(String command, Schema schema,
                             String dataSource, DataFormat dataFormat, List<String> uriList) throws DDFException;

    //public DDF sql2ddfHandle(String command, Schema schema,
    //                         String dataSource, DataFormat dataFormat, List<DDF> ddfList) throws DDFException;
}
