package dev.komu.ahwen.log

import dev.komu.ahwen.file.Page
import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.query.SqlInt
import dev.komu.ahwen.query.SqlString
import dev.komu.ahwen.types.SqlType

/**
 * Provides unstructured low-level read access to log records.
 *
 * Higher level log parsing can be implemented on top of this.
 */
class BasicLogRecord(private val page: Page, private var pos: Int) {

    fun nextInt(): Int =
        (nextValue(SqlType.INTEGER) as SqlInt).value

    fun nextString(): String =
        (nextValue(SqlType.VARCHAR) as SqlString).value

    private fun nextValue(type: SqlType): SqlValue {
        val result = page.getValue(pos, type)
        pos += result.representationSize
        return result
    }
}
