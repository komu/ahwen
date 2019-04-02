package dev.komu.ahwen.materialize

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan

class GroupByScan(
    private val scan: Scan,
    private val groupFields: Collection<String>,
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

    override fun getVal(fieldName: String): Constant {
        if (fieldName in groupFields)
            return groupValue[fieldName]

        return aggregationFns.first { it.fieldName == fieldName }.value
    }

    override fun hasField(fieldName: String): Boolean =
        fieldName in groupFields || aggregationFns.any { it.fieldName == fieldName }
}
