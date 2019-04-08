package dev.komu.ahwen.metadata

/**
 * Encapsulates statistics of a table.
 */
class StatInfo(val numBlocks: Int, val numRecords: Int) {

    /**
     * Estimate how many distinct values given field has.
     */
    fun distinctValues(fieldName: String): Int =
        // Calculate a crude approximation since we don't store better statistics.
        1 + (numRecords / 3)
}
