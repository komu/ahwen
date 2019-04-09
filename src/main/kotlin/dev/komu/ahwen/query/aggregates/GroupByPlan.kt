package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.materialize.SortPlan
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName

class GroupByPlan(
    plan: Plan,
    private val groupFields: Collection<ColumnName>,
    private val aggregationFns: Collection<AggregationFn>,
    tx: Transaction
) : Plan {

    private val plan = SortPlan(plan, groupFields.toList(), tx)

    override val schema = Schema {
        for (field in groupFields)
            copyFieldFrom(field, plan.schema)
        for (fn in aggregationFns)
            intField(fn.fieldName)
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

    override fun distinctValues(column: ColumnName): Int =
        if (column in plan.schema)
            plan.distinctValues(column)
        else
            recordsOutput
}
