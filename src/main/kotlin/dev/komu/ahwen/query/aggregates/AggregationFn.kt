package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan

interface AggregationFn {
    fun processFirst(scan: Scan)
    fun processNext(scan: Scan)
    val fieldName: String
    val value: Constant
}
