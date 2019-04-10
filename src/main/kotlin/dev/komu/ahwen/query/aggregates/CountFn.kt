package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.query.SqlInt
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

/**
 * Count rows in a group.
 */
class CountFn(column: ColumnName) : AggregationFn {

    override val columnName = ColumnName("countof$column")

    private var count = 0

    override fun processFirst(scan: Scan) {
        count = 1
    }

    override fun processNext(scan: Scan) {
        count++
    }

    override val value: SqlValue
        get() = SqlInt(count)
}
