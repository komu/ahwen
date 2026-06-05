package dev.komu.ahwen.log

import dev.komu.ahwen.file.Page
import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.types.SqlType

/**
 * Provides unstructured low-level read access to log records.
 *
 * Higher level log parsing can be implemented on top of this.
 */
class BasicLogRecord(private val page: Page, private var pos: Int) {

    fun nextInt(): Int =
        nextValue(SqlType.INTEGER).value

    fun nextString(): String =
        nextValue(SqlType.VARCHAR).value

    private fun <T : SqlValue> nextValue(type: SqlType<T>): T {
        val result = page.getValue(pos, type)
        pos += result.representationSize
        return result
    }
}
