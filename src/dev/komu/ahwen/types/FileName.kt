package dev.komu.ahwen.types

/**
 * Represents a name of a table.
 */
data class FileName(val value: String) : Comparable<FileName> {

    init {
        require(value.isNotEmpty()) { "empty table name" }
    }

    override fun toString() = value
    override fun compareTo(other: FileName) = value.compareTo(other.value)

    val isTemporary: Boolean
        get() = value.startsWith("temp")

}
