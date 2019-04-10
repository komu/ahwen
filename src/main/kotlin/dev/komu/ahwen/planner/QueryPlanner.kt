package dev.komu.ahwen.planner

import dev.komu.ahwen.parse.QueryData
import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.tx.Transaction

/**
 * Query planners take an AST representing the original SQL query and turn that
 * into an executable plan.
 */
interface QueryPlanner {
    fun createPlan(data: QueryData, tx: Transaction): Plan
}
