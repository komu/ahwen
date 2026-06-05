package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

/**
 * Calculates the maximum value of given column in group of rows.
 */
class MaxFn(private val column: ColumnName) : AggregationFn {

    override lateinit var value: SqlValue

    override fun processFirst(scan: Scan) {
        value = scan[column]
    }

    override fun processNext(scan: Scan) {
        value = maxOf(value, scan[column])
    }

    override val columnName = ColumnName("maxof$column")
}
