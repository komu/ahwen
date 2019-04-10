package dev.komu.ahwen.jdbc

import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.utils.unimplemented
import java.sql.ResultSet

class AhwenResultSet(private val rows: MutableList<Map<String, SqlValue>>) : ResultSet by unimplemented() {

    private var index = -1

    override fun next(): Boolean {
        if (index + 1 < rows.size) {
            index++
            return true
        } else {
            return false
        }
    }

    override fun getObject(columnLabel: String): Any {
        val value = rows[index][columnLabel] ?: error("unknown column $columnLabel")
        return value.value
    }

    override fun getInt(columnLabel: String): Int =
        getObject(columnLabel) as Int

    override fun getString(columnLabel: String): String =
        getObject(columnLabel) as String

    override fun close() {
    }

    override fun beforeFirst() {
        index = -1
    }
}
