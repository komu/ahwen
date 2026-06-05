package dev.komu.ahwen.query

import dev.komu.ahwen.types.ColumnName

/**
 * Runtime implementation of [ProjectPlan].
 */
class ProjectScan(private val scan: Scan, private val columns: Collection<ColumnName>) : Scan {

    override fun get(column: ColumnName): SqlValue =
        if (column in columns)
            scan[column]
        else
            error("field not found $column")

    override fun contains(column: ColumnName): Boolean =
        column in columns

    override fun beforeFirst() {
        scan.beforeFirst()
    }

    override fun next(): Boolean =
        scan.next()

    override fun close() {
        scan.close()
    }
}
