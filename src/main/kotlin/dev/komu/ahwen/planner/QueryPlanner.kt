package dev.komu.ahwen.planner

import dev.komu.ahwen.parse.QueryData
import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.tx.Transaction

interface QueryPlanner {
    fun createPlan(data: QueryData, tx: Transaction): Plan
}
