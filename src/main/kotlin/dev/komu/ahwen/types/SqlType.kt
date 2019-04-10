package dev.komu.ahwen.types

import dev.komu.ahwen.file.Page.Companion.INT_SIZE
import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.query.SqlInt
import dev.komu.ahwen.query.SqlString
import java.sql.Types
import java.sql.Types.VARCHAR
import java.time.chrono.JapaneseEra.values

sealed class SqlType(val code: Int) {
    object INTEGER : SqlType(Types.INTEGER)
    object VARCHAR : SqlType(Types.VARCHAR)

    /**
     * Returns the maximum amount of bytes to store a type of given logical length.
     */
    fun maximumBytes(length: Int): Int = when (this) {
        INTEGER -> INT_SIZE
        VARCHAR -> INT_SIZE + (length * MAX_BYTES_PER_CHAR) // storage format: length + data
    }

    val defaultValue: SqlValue
        get() = when (this) {
            INTEGER -> SqlInt(0)
            VARCHAR -> SqlString("")
        }

    val minimumValue: SqlValue
        get() = when (this) {
            INTEGER -> SqlInt(Int.MIN_VALUE)
            VARCHAR -> SqlString("")
        }

    companion object {

        operator fun invoke(code: Int): SqlType = when (code) {
            Types.INTEGER -> INTEGER
            Types.VARCHAR -> VARCHAR
            else -> error("invalid type-code: $code")
        }

        private val MAX_BYTES_PER_CHAR = SqlString.charset.newEncoder().maxBytesPerChar().toInt()
    }
}
