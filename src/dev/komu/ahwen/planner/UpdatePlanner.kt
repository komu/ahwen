package dev.komu.ahwen.planner

import dev.komu.ahwen.parse.*
import dev.komu.ahwen.tx.Transaction

/**
 * Update planners are similar to [QueryPlanner]s, but have a bit more responsibilities.
 * Instead of simply converting query into an executable plan, they also execute the plan
 * and update indices that are affected by the execution.
 */
interface UpdatePlanner {
    fun executeInsert(data: InsertData, tx: Transaction): Int
    fun executeDelete(data: DeleteData, tx: Transaction): Int
    fun executeModify(data: ModifyData, tx: Transaction): Int
    fun executeCreateTable(data: CreateTableData, tx: Transaction): Int
    fun executeCreateView(data: CreateViewData, tx: Transaction): Int
    fun executeCreateIndex(data: CreateIndexData, tx: Transaction): Int
}
