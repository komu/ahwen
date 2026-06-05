package dev.komu.ahwen.types

/**
 * Represents a name of a file.
 */
data class IndexName(val value: String) : Comparable<IndexName> {

    init {
        require(value.isNotEmpty()) { "empty file name" }
    }

    override fun toString() = value
    override fun compareTo(other: IndexName) = value.compareTo(other.value)
}
