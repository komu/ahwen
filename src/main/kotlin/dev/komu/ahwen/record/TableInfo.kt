package dev.komu.ahwen.record

import java.sql.Types

class TableInfo(
    val tableName: String,
    val schema: Schema,
    private val offsets: Map<String, Int>,
    val recordLength: Int) {

    val fileName: String
        get() = "$tableName.tbl"

    fun offset(name: String): Int =
        offsets[name] ?: error("no field $name in $tableName")

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
                Types.INTEGER -> Int.SIZE_BYTES
                Types.VARCHAR -> length(fieldName)
                else -> error("invalid type $type for $fieldName")
            }
        }
    }
}
