package io.ddf2.datasource;

import io.ddf2.datasource.schema.Schema;

import java.util.List;

/**
 * Created by sangdn on 12/30/15.
 */

/**
 * Responsible to resolve file format to Schema
 */
public interface IFileFormatResolver {
    Schema resolve(List<String> preferColumnName,List<String> sampleRows);
}
