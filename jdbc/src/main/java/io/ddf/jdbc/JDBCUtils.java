package io.ddf.jdbc;


import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

import java.sql.Types;

/**
 * Created by freeman on 7/21/15.
 */
public class JDBCUtils {
  public static String getSqlTypeName(int type) {
    switch (type) {
      case Types.BIT:
        return "BIT";
      case Types.TINYINT:
        return "TINYINT";
      case Types.SMALLINT:
        return "SMALLINT";
      case Types.INTEGER:
        return "INTEGER";
      case Types.BIGINT:
        return "BIGINT";
      case Types.FLOAT:
        return "FLOAT";
      case Types.REAL:
        return "REAL";
      case Types.DOUBLE:
        return "DOUBLE";
      case Types.NUMERIC:
        return "NUMERIC";
      case Types.DECIMAL:
        return "DECIMAL";
      case Types.CHAR:
        return "CHAR";
      case Types.VARCHAR:
        return "VARCHAR";
      case Types.LONGVARCHAR:
        return "LONGVARCHAR";
      case Types.DATE:
        return "DATE";
      case Types.TIME:
        return "TIME";
      case Types.TIMESTAMP:
        return "TIMESTAMP";
      case Types.BINARY:
        return "BINARY";
      case Types.VARBINARY:
        return "VARBINARY";
      case Types.LONGVARBINARY:
        return "LONGVARBINARY";
      case Types.NULL:
        return "NULL";
      case Types.OTHER:
        return "OTHER";
      case Types.JAVA_OBJECT:
        return "JAVA_OBJECT";
      case Types.DISTINCT:
        return "DISTINCT";
      case Types.STRUCT:
        return "STRUCT";
      case Types.ARRAY:
        return "ARRAY";
      case Types.BLOB:
        return "BLOB";
      case Types.CLOB:
        return "CLOB";
      case Types.REF:
        return "REF";
      case Types.DATALINK:
        return "DATALINK";
      case Types.BOOLEAN:
        return "BOOLEAN";
      case Types.ROWID:
        return "ROWID";
      case Types.NCHAR:
        return "NCHAR";
      case Types.NVARCHAR:
        return "NVARCHAR";
      case Types.LONGNVARCHAR:
        return "LONGNVARCHAR";
      case Types.NCLOB:
        return "NCLOB";
      case Types.SQLXML:
        return "SQLXML";
    }

    return "?";
  }

  /**
   *
   * @return DDF Column type
   */
  public static Schema.ColumnType getDDFType(Integer colType) throws DDFException {
    switch(colType) {
      case Types.ARRAY: return Schema.ColumnType.ARRAY;
      case Types.BIGINT:  return Schema.ColumnType.BIGINT;
      case Types.BINARY: return Schema.ColumnType.BINARY;
      case Types.BOOLEAN: return Schema.ColumnType.BOOLEAN;
      case Types.BIT: return Schema.ColumnType.BOOLEAN; //TODO: verify
      case Types.CHAR: return Schema.ColumnType.STRING;
      case Types.DATE: return Schema.ColumnType.DATE;
      case Types.DECIMAL: return Schema.ColumnType.DECIMAL;
      case Types.DOUBLE: return Schema.ColumnType.DOUBLE;
      case Types.FLOAT: return Schema.ColumnType.FLOAT;
      case Types.INTEGER: return Schema.ColumnType.INT;
      case Types.LONGVARCHAR: return Schema.ColumnType.STRING; //TODO: verify
      case Types.NUMERIC: return Schema.ColumnType.DECIMAL;
      case Types.NVARCHAR: return Schema.ColumnType.STRING; //TODO: verify
      case Types.SMALLINT: return Schema.ColumnType.INT;
      case Types.TIMESTAMP: return Schema.ColumnType.TIMESTAMP;
      case Types.TINYINT: return Schema.ColumnType.INT;
      case Types.VARCHAR: return Schema.ColumnType.STRING; //TODO: verify
      case Types.VARBINARY: return Schema.ColumnType.BINARY;
      default: throw new DDFException(String.format("Type not support %s", JDBCUtils.getSqlTypeName(colType)));
        //TODO: complete for other types
    }
  }

}
