package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

class MaxFn(private val column: ColumnName) : AggregationFn {

    // TODO: once we support nulls, this should start with a null value
    override lateinit var value: Constant

    override fun processFirst(scan: Scan) {
        value = scan[column]
    }

    override fun processNext(scan: Scan) {
        value = maxOf(value, scan[fieldName])
    }

    override val fieldName = ColumnName("maxof$column")
}
