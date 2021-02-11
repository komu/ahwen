package dev.komu.ahwen.planner

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.metadata.MetadataManager
import dev.komu.ahwen.parse.QueryData
import dev.komu.ahwen.parse.SelectExp
import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.ProjectPlan
import dev.komu.ahwen.query.aggregates.AggregationFn
import dev.komu.ahwen.query.aggregates.CountFn
import dev.komu.ahwen.query.aggregates.GroupByPlan
import dev.komu.ahwen.query.aggregates.MaxFn
import dev.komu.ahwen.query.materialize.SortPlan
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName

/**
 * [QueryPlanner] that uses simple heuristics to decide on a good join order for tables:
 *
 * First the table that would result in lowest number of outputted rows is picked as the initial table.
 * Then as long as there are more tables available, joins the one whose output would result in the lowest
 * amount of rows outputted after the join.
 */
class HeuristicQueryPlanner(
    private val metadataManager: MetadataManager,
    private val bufferManager: BufferManager
) : QueryPlanner {

    override fun createPlan(data: QueryData, tx: Transaction): Plan {
        // TODO: support views
        val tablePlanners = data.tables.map { TablePlanner(it, data.predicate, tx, metadataManager, bufferManager) }
            .toMutableList()

        var currentPlan = tablePlanners.getLowestSelectPlan()

        while (!tablePlanners.isEmpty())
            currentPlan = tablePlanners.getLowestJoinPlan(currentPlan) ?: tablePlanners.getLowestProductPlan(currentPlan)

        val aggregates = data.selectExps.filterIsInstance<SelectExp.Aggregate>().map { createAggregateFn(it.fn, it.column) }

        if (data.groupBy.isNotEmpty()) {
            currentPlan = GroupByPlan(currentPlan, data.groupBy, aggregates, tx)
        } else {
            check(aggregates.isEmpty()) { "specified aggregate functions without group by"}
        }

        if (data.orderBy.isNotEmpty())
            currentPlan = SortPlan(currentPlan, data.orderBy, tx)

        val columns = data.selectExps.filterIsInstance<SelectExp.Column>().map { it.column } + aggregates.map { it.columnName }

        return ProjectPlan(currentPlan, columns)
    }

    companion object {

        private fun createAggregateFn(name: String, column: ColumnName): AggregationFn = when (name) {
            "count" -> CountFn(column)
            "max" -> MaxFn(column)
            else -> error("unknown aggregate function '$name'")
        }

        private fun MutableList<TablePlanner>.getLowestSelectPlan(): Plan {
            val (bestPlanner, bestPlan) = this
                .map { it to it.makeSelectPlan() }
                .minByOrNull { (_, plan) -> plan.recordsOutput } ?: error("no planners")

            remove(bestPlanner)
            return bestPlan
        }

        private fun MutableList<TablePlanner>.getLowestJoinPlan(currentPlan: Plan): Plan? {
            val (bestPlanner, bestPlan) = this
                .map { it to it.makeJoinPlan(currentPlan) }
                .filter { (_, plan) -> plan != null }
                .minByOrNull { (_, plan) -> plan!!.recordsOutput }
                ?: return null

            remove(bestPlanner)
            return bestPlan
        }

        private fun MutableList<TablePlanner>.getLowestProductPlan(currentPlan: Plan): Plan {
            val (bestPlanner, bestPlan) = this
                .map { it to it.makeProductPlan(currentPlan) }
                .minByOrNull { (_, plan) -> plan.recordsOutput }
                ?: error("no planners")

            remove(bestPlanner)
            return bestPlan
        }
    }
}
