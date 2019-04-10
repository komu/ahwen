package dev.komu.ahwen.record

import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.FileName
import dev.komu.ahwen.types.SqlType.INTEGER
import dev.komu.ahwen.types.SqlType.VARCHAR
import dev.komu.ahwen.types.TableName

/**
 * Represents the physical layout of a table, mapping columns to their offsets.
 *
 * @see Schema
 */
class TableInfo(
    val tableName: TableName,
    val schema: Schema,
    private val offsets: Map<ColumnName, Int>,
    val recordLength: Int) {

    val fileName = FileName("$tableName.tbl")

    fun offset(name: ColumnName): Int =
        offsets[name] ?: error("no field $name in $tableName")

    /**
     * Opens a cursor into this file
     */
    fun open(tx: Transaction) =
        RecordFile(this, tx)

    companion object {

        operator fun invoke(tableName: TableName, schema: Schema): TableInfo {
            val offsets = mutableMapOf<ColumnName, Int>()
            var pos = 0
            for (column in schema.columns) {
                offsets[column] = pos
                pos += schema.lengthInBytes(column)
            }

            return TableInfo(tableName, schema, offsets, pos)
        }
    }
}
