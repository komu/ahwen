package dev.komu.ahwen.query.aggregates

import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.materialize.SortPlan
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName

/**
 * Plan for [GroupByScan].
 *
 * First sorts the input by group key and then calculates the aggregates on the fly.
 */
class GroupByPlan(
    plan: Plan,
    private val groupFields: List<ColumnName>,
    private val aggregationFns: Collection<AggregationFn>,
    tx: Transaction
) : Plan {

    private val sortPlan = SortPlan(plan, groupFields, tx)

    override val schema = Schema {
        for (field in groupFields)
            copyFieldFrom(field, plan.schema)
        for (fn in aggregationFns)
            intField(fn.columnName)
    }

    override fun open(): Scan =
        GroupByScan(sortPlan.open(), groupFields, aggregationFns)

    override val blocksAccessed: Int
        get() = sortPlan.blocksAccessed

    override val recordsOutput: Int
        get() {
            var result = 1
            for (fieldName in groupFields)
                result *= sortPlan.distinctValues(fieldName)
            return result
        }

    override fun distinctValues(column: ColumnName): Int =
        if (column in sortPlan.schema)
            sortPlan.distinctValues(column)
        else
            recordsOutput
}
