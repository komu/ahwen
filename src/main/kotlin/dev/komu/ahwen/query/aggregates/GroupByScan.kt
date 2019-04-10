package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

/**
 * A scan that performs group by with given aggregate functions.
 */
class GroupByScan(
    private val scan: Scan,
    private val groupFields: Collection<ColumnName>,
    private val aggregationFns: Collection<AggregationFn>
) : Scan {

    private lateinit var groupValue: GroupValue
    private var moreGroups = false

    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        scan.beforeFirst()
        moreGroups = scan.next()
    }

    override fun next(): Boolean {
        if (!moreGroups)
            return false

        for (fn in aggregationFns)
            fn.processFirst(scan)

        groupValue = GroupValue(scan, groupFields)
        moreGroups = scan.next()
        while (moreGroups) {
            val gv = GroupValue(scan, groupFields)
            if (gv != groupValue)
                break

            for (fn in aggregationFns)
                fn.processNext(scan)

            moreGroups = scan.next()
        }

        return true
    }

    override fun close() {
        scan.close()
    }

    override fun get(column: ColumnName): SqlValue =
        if (column in groupFields)
            groupValue[column]
        else
            aggregationFns.first { it.columnName == column }.value

    override fun contains(column: ColumnName): Boolean =
        column in groupFields || aggregationFns.any { it.columnName == column }

    private data class GroupValue(private val values: Map<ColumnName, SqlValue>) {

        constructor(scan: Scan, fields: Collection<ColumnName>) :
            this(fields.map { it to scan[it] }.toMap())

        operator fun get(fieldName: ColumnName): SqlValue =
            values[fieldName] ?: error("unknown field $fieldName")
    }
}
