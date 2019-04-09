package dev.komu.ahwen.query

import dev.komu.ahwen.types.ColumnName

class SelectPlan(private val plan: Plan, private val predicate: Predicate) : Plan by plan {

    override fun open(): UpdateScan =
        SelectScan(plan.open(), predicate)

    override val recordsOutput: Int
        get() = plan.recordsOutput / predicate.reductionFactor(plan)

    override fun distinctValues(column: ColumnName): Int =
        if (predicate.equatesWithConstant(column) != null)
            1
        else
            minOf(plan.distinctValues(column), recordsOutput)

    override fun toString() =
        "[SelectPlan plan=$plan, predicate=$predicate]"
}
