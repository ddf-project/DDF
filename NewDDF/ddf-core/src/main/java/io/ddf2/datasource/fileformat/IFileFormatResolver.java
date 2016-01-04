package io.ddf2.datasource.fileformat;

import io.ddf2.datasource.schema.ISchema;

import java.util.List;

/**
 * Created by sangdn on 12/30/15.
 */

/**
 * Responsible to resolve @IFileFormat to ISchema
 */
public interface IFileFormatResolver {
    ISchema resolve(List<String> preferColumnName,List<List<String>> sampleRows);
}
