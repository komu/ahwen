@file:Suppress("DataClassPrivateConstructor")

package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan

data class GroupValue
    private constructor(private val values: Map<String, Constant>) {

    constructor(scan: Scan, fields: Collection<String>) :
            this(fields.map { it to scan.getVal(it) }.toMap())

    operator fun get(fieldName: String): Constant =
        values[fieldName] ?: error("unknown field $fieldName")
}
