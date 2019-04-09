package dev.komu.ahwen.query

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.query.materialize.MaterializePlan
import dev.komu.ahwen.query.materialize.TempTable
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName

class MultiBufferProductPlan(
    private val lhs: Plan,
    private val rhs: Plan,
    private val tx: Transaction,
    private val bufferManager: BufferManager
) : Plan {

    override val schema = lhs.schema + rhs.schema

    override fun open(): Scan {
        val tt = copyRecordsFrom(rhs)
        val leftScan = lhs.open()
        return MultiBufferProductScan(leftScan, tt.tableInfo, tx, bufferManager.available)
    }

    override val blocksAccessed: Int
        get() {
            val available = bufferManager.available
            val size = MaterializePlan(rhs, tx).blocksAccessed
            val numChunks = size / available.coerceAtLeast(1)
            return rhs.blocksAccessed + (lhs.blocksAccessed * numChunks)
        }

    override val recordsOutput: Int
        get() = lhs.recordsOutput * rhs.recordsOutput

    override fun distinctValues(column: ColumnName): Int =
        if (column in lhs.schema) lhs.distinctValues(column) else rhs.distinctValues(column)

    private fun copyRecordsFrom(plan: Plan): TempTable {
        val schema = plan.schema
        val tt = TempTable(schema, tx)
        tt.open().use { dest ->
            dest.copyFrom(plan, schema)
        }
        return tt
    }

    override fun toString() =
        "[MultiBufferProductPlan lhs=$lhs, rhs=$rhs]"
}
