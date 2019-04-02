package dev.komu.ahwen.materialize

import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction

class GroupByPlan(
    plan: Plan,
    private val groupFields: Collection<String>,
    private val aggregationFns: Collection<AggregationFn>,
    tx: Transaction
) : Plan {

    private val plan = SortPlan(plan, groupFields.toList(), tx)

    override val schema = Schema().apply {
        for (field in groupFields)
            add(field, plan.schema)
        for (fn in aggregationFns)
            addIntField(fn.fieldName)
    }

    override fun open(): Scan =
        GroupByScan(plan.open(), groupFields, aggregationFns)

    override val blocksAccessed: Int
        get() = plan.blocksAccessed

    override val recordsOutput: Int
        get() {
            var result = 1
            for (fieldName in groupFields)
                result *= plan.distinctValues(fieldName)
            return result
        }

    override fun distinctValues(fieldName: String): Int =
        if (plan.schema.hasField(fieldName))
            plan.distinctValues(fieldName)
        else
            recordsOutput
}
