@file:Suppress("DataClassPrivateConstructor")

package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

data class GroupValue
    private constructor(private val values: Map<ColumnName, Constant>) {

    constructor(scan: Scan, fields: Collection<ColumnName>) :
            this(fields.map { it to scan[it] }.toMap())

    operator fun get(fieldName: ColumnName): Constant =
        values[fieldName] ?: error("unknown field $fieldName")
}
