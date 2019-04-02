package dev.komu.ahwen.index.query

import dev.komu.ahwen.index.Index
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.TableScan

class IndexJoinScan(
    private val scan: Scan,
    private val index: Index,
    private val joinField: String,
    private val tableScan: TableScan
) : Scan {

    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        scan.beforeFirst()
        scan.next()
        resetIndex()
    }

    override fun next(): Boolean {
        while (true) {
            if (index.next()) {
                tableScan.moveToRid(index.dataRid)
                return true
            }

            if (!scan.next())
                return false
            resetIndex()
        }
    }

    override fun close() {
        scan.close()
        tableScan.close()
        index.close()
    }

    override fun getVal(fieldName: String): Constant =
        if (tableScan.hasField(fieldName)) tableScan.getVal(fieldName) else scan.getVal(fieldName)

    override fun hasField(fieldName: String): Boolean =
        tableScan.hasField(fieldName) || scan.hasField(fieldName)

    private fun resetIndex() {
        index.beforeFirst(scan.getVal(joinField))
    }
}
