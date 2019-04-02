package dev.komu.ahwen.index.query

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.metadata.MetadataManager
import dev.komu.ahwen.opt.TablePlanner
import dev.komu.ahwen.parse.QueryData
import dev.komu.ahwen.planner.QueryPlanner
import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.ProjectPlan
import dev.komu.ahwen.tx.Transaction

class HeuristicQueryPlanner(
    private val metadataManager: MetadataManager,
    private val bufferManager: BufferManager
) : QueryPlanner {

    override fun createPlan(data: QueryData, tx: Transaction): Plan {
        val tablePlanners = mutableListOf<TablePlanner>()

        for (tblname in data.tables)
            tablePlanners += TablePlanner(tblname, data.predicate, tx, metadataManager, bufferManager)

        var currentPlan = tablePlanners.getLowestSelectPlan()

        while (!tablePlanners.isEmpty())
            currentPlan = tablePlanners.getLowestJoinPlan(currentPlan) ?: tablePlanners.getLowestProductPlan(currentPlan)

        println(currentPlan)
        return ProjectPlan(currentPlan, data.fields)
    }

    companion object {

        private fun MutableList<TablePlanner>.getLowestSelectPlan(): Plan {
            val (bestPlanner, bestPlan) = this
                .map { it to it.makeSelectPlan() }
                .minBy { (_, plan) -> plan.recordsOutput } ?: error("no planners")

            remove(bestPlanner)
            return bestPlan
        }

        private fun MutableList<TablePlanner>.getLowestJoinPlan(currentPlan: Plan): Plan? {
            val (bestPlanner, bestPlan) = this
                .map { it to it.makeJoinPlan(currentPlan) }
                .filter { (_, plan) -> plan != null }
                .minBy { (_, plan) -> plan!!.recordsOutput }
                ?: return null

            remove(bestPlanner)
            return bestPlan
        }

        private fun MutableList<TablePlanner>.getLowestProductPlan(currentPlan: Plan): Plan {
            val (bestPlanner, bestPlan) = this
                .map { it to it.makeProductPlan(currentPlan) }
                .minBy { (_, plan) -> plan.recordsOutput }
                ?: error("no planners")

            remove(bestPlanner)
            return bestPlan
        }
    }
}
