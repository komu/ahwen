package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan

class MaxFn(private val field: String) : AggregationFn {

    // TODO: once we support nulls, this should start with a null value
    override lateinit var value: Constant

    override fun processFirst(scan: Scan) {
        value = scan.getVal(field)
    }

    override fun processNext(scan: Scan) {
        value = maxOf(value, scan.getVal(fieldName))
    }

    override val fieldName: String = "maxof$field"
}
