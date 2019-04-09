package dev.komu.ahwen.types

/**
 * Represents a name of a column.
 */
data class ColumnName(val value: String) : Comparable<ColumnName> {

    init {
        require(value.isNotEmpty()) { "empty column name" }
    }

    override fun toString() = value
    override fun compareTo(other: ColumnName) = value.compareTo(other.value)
}
