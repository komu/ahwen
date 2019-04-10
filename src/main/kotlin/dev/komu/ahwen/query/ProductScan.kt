package dev.komu.ahwen.query

import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.query.materialize.MultiBufferProductPlan

/**
 * Evaluates the cartesian product of [s1] and [s2] using nested loops.
 *
 * @see MultiBufferProductPlan
 */
class ProductScan(private val s1: Scan, private val s2: Scan) : Scan {
    init {
        s1.next()
    }

    override fun beforeFirst() {
        s1.beforeFirst()
        s1.next()
        s2.beforeFirst()
    }

    override fun next() =
        if (s2.next()) {
            true
        } else {
            s2.beforeFirst()
            s2.next() && s1.next()
        }

    override fun close() {
        s1.close()
        s2.close()
    }

    override fun get(column: ColumnName): SqlValue =
        if (column in s1) s1[column] else s2[column]

    override fun contains(column: ColumnName): Boolean =
        column in s1 || column in s2
}
