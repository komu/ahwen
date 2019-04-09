package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

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

    override fun get(column: ColumnName): Constant {
        if (column in groupFields)
            return groupValue[column]

        return aggregationFns.first { it.fieldName == column }.value
    }

    override fun contains(column: ColumnName): Boolean =
        column in groupFields || aggregationFns.any { it.fieldName == column }
}
