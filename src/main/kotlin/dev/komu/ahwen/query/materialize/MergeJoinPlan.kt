package dev.komu.ahwen.query.materialize

import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName

/**
 * Joins tables using merge join.
 *
 * Merge join sorts both inputs by the join keys and then proceeds to iterate through them:
 * since both inputs are sorted, it only needs to look at first keys of both inputs to consider
 * whether they can be joined or whether other one (and which one) needs to be advanced.
 */
class MergeJoinPlan(
    p1: Plan,
    p2: Plan,
    private val joinColumn1: ColumnName,
    private val joinColumn2: ColumnName,
    tx: Transaction
) : Plan {

    private val p1 = SortPlan(p1, listOf(joinColumn1), tx)
    private val p2 = SortPlan(p2, listOf(joinColumn2), tx)

    override val schema = p1.schema + p2.schema

    override fun open(): Scan {
        val s1 = p1.open()
        val s2 = p2.open()

        return MergeJoinScan(s1, s2, joinColumn1, joinColumn2)
    }

    override val blocksAccessed: Int
        get() = p1.blocksAccessed + p2.blocksAccessed

    override val recordsOutput: Int
        get() {
            val maxVals = maxOf(p1.distinctValues(joinColumn1), p2.distinctValues(joinColumn2))
            return (p1.recordsOutput * p2.recordsOutput) / maxVals
        }

    override fun distinctValues(column: ColumnName): Int =
        if (column in p1.schema)
            p1.distinctValues(column)
        else
            p2.distinctValues(column)
}
