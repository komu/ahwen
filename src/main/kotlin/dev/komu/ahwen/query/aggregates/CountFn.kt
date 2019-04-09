package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.IntConstant
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

class CountFn(column: ColumnName) : AggregationFn {

    override val fieldName = ColumnName("countof$column")

    private var count = 0

    override fun processFirst(scan: Scan) {
        count = 1
    }

    override fun processNext(scan: Scan) {
        count++
    }

    override val value: Constant
        get() = IntConstant(count)
}
