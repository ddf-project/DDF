package io.ddf.etl;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.datasource.DataFormat;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.List;
import java.util.UUID;

public interface IHandleSql extends IHandleSqlLike, IHandleDDFFunctionalGroup {
    /**
     * @brief This function is used to handle user-input sql. It will (1) handle ddl.
     * (2) reformulate sql with table name.
     * @param command The sql command. It should contains full uri as reference in the command.
     *                For example, "select * from ddf://adatao/ddfname". Or the sqlcmd doesn't
     *                need any conversion if it directly refer to the table name in the sql
     *                engine. For example, "select * from airline" where airline is a table in
     *                sql engine.
     * @param maxRows
     * @param dataSource The dataSource (URI) of the data, e.g., jdbc://xxx
     * @return The query result.
     * @throws DDFException
     */
    public SqlResult sqlHandle(String command,
                               Integer maxRows,
                               String dataSource) throws DDFException;

    /**
     * @brief This function is used to handle user-input sql.
     * @param command The sql command. It should contains ddfname as reference in the command.
     *                For example, "select * from ddfname".
     * @param maxRows
     * @param dataSource The dataSource (URI) of the data, e.g., jdbc://xxx
     * @param namespace The namespace.
     * @return The query result.
     * @throws DDFException
     */
    public SqlResult sqlHandle(String command,
                               Integer maxRows,
                               String dataSource,
                               String namespace) throws DDFException;


    /**
     * @brief The function is used to handle user-input sql.
     * @param command The sql command. It should contains number as reference in the command.
     *                For example, "select {1}.year from {1}".
     * @param maxRows
     * @param dataSource The dataSource (URI) of the data, e.g., jdbc://xxx
     * @param uriList The list of the uris. The index begins from 1. For example,
     *                sqlcmd = "select {1}.year from {1}"
     *                uriList = {"ddf://adatao/airline", "ddf://adatao/train"}
     *                It should return the same result as :
     *                "select ddf://adatao/airline.year from ddf://adatao/year"
     * @return
     * @throws DDFException
     */
    public SqlResult sqlHandle(String command,
                               Integer maxRows,
                               String dataSource,
                               List<String> uriList) throws DDFException;

    /**
     * @brief The function is used to handle user-input sql.
     * @param command
     * @param maxRows
     * @param dataSource
     * @param uuidList The list of uuids.
     * @return
     * @throws DDFException
     */
    public SqlResult sqlHandle(String command,
                               Integer maxRows,
                               String dataSource,
                               UUID[] uuidList) throws DDFException;

    /**
     * @brief The function is used to create ddf from sqlcmd. The namespace, uriList, and uuidList
     * have similar definition as sql().
     * @param command The sql command.
     * @param schema     If {@link Schema} is null, then the data is expected to have {@link Schema}
     *                   information available
     * @param dataSource The dataSource (URI) of the data, e.g., jdbc://xxx
     * @param dataFormat
     */
    public DDF sql2ddfHandle(String command,
                             Schema schema,
                             String dataSource,
                             DataFormat dataFormat) throws DDFException;

    public DDF sql2ddfHandle(String command,
                             Schema schema,
                             String dataSource,
                             DataFormat dataFormat,
                             String namespace) throws DDFException;

    public DDF sql2ddfHandle(String command,
                             Schema schema,
                             String dataSource,
                             DataFormat dataFormat,
                             List<String> uriList) throws DDFException;

    public DDF sql2ddfHandle(String command,
                             Schema schema,
                             String dataSource,
                             DataFormat dataFormat,
                             UUID[] uuidList) throws DDFException;

}
