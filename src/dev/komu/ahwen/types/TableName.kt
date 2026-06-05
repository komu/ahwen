package dev.komu.ahwen.types

/**
 * Represents a name of a table.
 */
data class TableName(val value: String) : Comparable<TableName> {

    init {
        require(value.isNotEmpty()) { "empty table name" }
    }

    override fun toString() = value
    override fun compareTo(other: TableName) = value.compareTo(other.value)

    companion object {
        fun temporary(num: Int): TableName =
            TableName("temp$num")

        val DUMMY = TableName("-")
    }
}
