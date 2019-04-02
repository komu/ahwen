package dev.komu.ahwen.materialize

import dev.komu.ahwen.query.TableScan
import dev.komu.ahwen.query.UpdateScan
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import java.util.concurrent.atomic.AtomicInteger

class TempTable(schema: Schema, private val tx: Transaction) {

    val tableInfo = TableInfo(nextTableName(), schema)

    fun open(): UpdateScan =
        TableScan(tableInfo, tx)

    companion object {

        private val nextTableNum = AtomicInteger(0)

        private fun nextTableName() =
            "temp${nextTableNum.incrementAndGet()}"
    }
}
