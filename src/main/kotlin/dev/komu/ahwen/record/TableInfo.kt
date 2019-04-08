package dev.komu.ahwen.record

import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.SqlType

/**
 * Represents the physical layout of a table, mapping fields to their offsets.
 *
 * @see Schema
 */
class TableInfo(
    val tableName: String,
    val schema: Schema,
    private val offsets: Map<String, Int>,
    val recordLength: Int) {

    val fileName: String
        get() = "$tableName.tbl"

    fun offset(name: String): Int =
        offsets[name] ?: error("no field $name in $tableName")

    /**
     * Opens a cursor into this file
     */
    fun open(tx: Transaction) =
        RecordFile(this, tx)

    companion object {

        operator fun invoke(tableName: String, schema: Schema): TableInfo {
            val offsets = mutableMapOf<String, Int>()
            var pos = 0
            for (fieldName in schema.fields) {
                offsets[fieldName] = pos
                pos += schema.lengthInBytes(fieldName)
            }

            return TableInfo(tableName, schema, offsets, pos)
        }

        private fun Schema.lengthInBytes(fieldName: String): Int {
            val type = type(fieldName)
            return when (type) {
                SqlType.INTEGER -> Int.SIZE_BYTES
                SqlType.VARCHAR -> Int.SIZE_BYTES + length(fieldName)
            }
        }
    }
}
