package dev.komu.ahwen.metadata

import dev.komu.ahwen.types.ColumnName

/**
 * Encapsulates statistics of a table.
 */
class StatInfo(val numBlocks: Int, val numRecords: Int) {

    /**
     * Estimate how many distinct values given field has.
     */
    fun distinctValues(@Suppress("UNUSED_PARAMETER") fieldName: ColumnName): Int =
        // Calculate a crude approximation since we don't store better statistics.
        1 + (numRecords / 3)
}
