package io.ddf.content;

import io.ddf.content.Schema.ColumnType;
/**
 * Created by jing on 6/30/15.
 * This class is for cell structure is a table.
 */
public class SqlTypedCell {
    // The type of the value.
    private ColumnType valueType;
    // The value.
    private String value;

    /**
     * @brief Constructor.
     * @param valueType The type of the value.
     * @param value The value of this cell.
     */
    public SqlTypedCell(ColumnType valueType, String value) {
        this.valueType = valueType;
        this.value = value;
    }

    /**
     * @brief Getters and Setters.
     */
    public ColumnType getValueType() {
        return this.valueType;
    }

    public void setValueType(ColumnType valueType) {
        this.valueType = valueType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
