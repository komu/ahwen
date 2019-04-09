package dev.komu.ahwen.index.query

import dev.komu.ahwen.index.Index
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.TableScan
import dev.komu.ahwen.types.ColumnName

class IndexSelectScan(
    private val index: Index,
    private val value: Constant,
    private val tableScan: TableScan
) : Scan {

    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        index.beforeFirst(value)
    }

    override fun next(): Boolean {
        val ok = index.next()
        if (ok)
            tableScan.moveToRid(index.dataRid)
        return ok
    }

    override fun close() {
        index.close()
        tableScan.close()
    }

    override fun get(column: ColumnName): Constant =
        tableScan[column]

    override fun contains(column: ColumnName): Boolean =
        column in tableScan
}
