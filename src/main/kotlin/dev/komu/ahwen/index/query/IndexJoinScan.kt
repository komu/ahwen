package dev.komu.ahwen.index.query

import dev.komu.ahwen.index.Index
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.TableScan
import dev.komu.ahwen.types.ColumnName

class IndexJoinScan(
    private val scan: Scan,
    private val index: Index,
    private val joinField: ColumnName,
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

    override fun get(column: ColumnName): Constant =
        if (column in tableScan) tableScan[column] else scan[column]

    override fun contains(column: ColumnName): Boolean =
        column in tableScan || column in scan

    private fun resetIndex() {
        index.beforeFirst(scan[joinField])
    }
}
