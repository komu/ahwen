package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

interface AggregationFn {
    fun processFirst(scan: Scan)
    fun processNext(scan: Scan)
    val fieldName: ColumnName
    val value: Constant
}
