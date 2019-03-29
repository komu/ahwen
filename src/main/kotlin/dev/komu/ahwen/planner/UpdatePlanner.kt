package dev.komu.ahwen.planner

import dev.komu.ahwen.parse.*
import dev.komu.ahwen.tx.Transaction

interface UpdatePlanner {
    fun executeInsert(data: InsertData, tx: Transaction): Int
    fun executeDelete(data: DeleteData, tx: Transaction): Int
    fun executeModify(data: ModifyData, tx: Transaction): Int
    fun executeCreateTable(data: CreateTableData, tx: Transaction): Int
    fun executeCreateView(data: CreateViewData, tx: Transaction): Int
    fun executeCreateIndex(data: CreateIndexData, tx: Transaction): Int
}
