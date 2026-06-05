package dev.komu.ahwen.query

import dev.komu.ahwen.types.SqlType

/**
 * Base class for valid SQL values. Each value has an associated [SqlType].
 */
sealed class SqlValue : Comparable<SqlValue> {
    abstract val value: Any
    abstract val type: SqlType<SqlValue>

    /** How many bytes are needed to store this value? */
    abstract val representationSize: Int
}

data class SqlInt(override val value: Int) : SqlValue() {

    override val type: SqlType<SqlInt>
        get() = SqlType.INTEGER

    override val representationSize: Int
        get() = Int.SIZE_BYTES

    override fun compareTo(other: SqlValue): Int =
        value.compareTo((other as SqlInt).value)

    override fun toString(): String = value.toString()
}

data class SqlString(override val value: String) : SqlValue() {

    override val type: SqlType<SqlString>
        get() = SqlType.VARCHAR

    override val representationSize: Int
        get() = type.maximumBytes(value.length) // TODO: consider returning actual size instead of upper bound

    override fun compareTo(other: SqlValue): Int =
        value.compareTo((other as SqlString).value)

    override fun toString(): String = value

    companion object {
        /** Character set used to store strings */
        val charset = Charsets.UTF_8
    }
}
