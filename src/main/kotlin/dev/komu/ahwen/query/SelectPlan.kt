package dev.komu.ahwen.query

class SelectPlan(private val plan: Plan, private val predicate: Predicate) : Plan by plan {

    override fun open(): UpdateScan =
        SelectScan(plan.open(), predicate)

    override val recordsOutput: Int
        get() = plan.recordsOutput / predicate.reductionFactor(plan)

    override fun distinctValues(fieldName: String): Int =
        if (predicate.equatesWithConstant(fieldName) != null)
            1
        else
            minOf(plan.distinctValues(fieldName), recordsOutput)

    override fun toString() =
        "[SelectPlan plan=$plan, predicate=$predicate]"
}
