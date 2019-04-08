package dev.komu.ahwen.query

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.query.materialize.MaterializePlan
import dev.komu.ahwen.query.materialize.TempTable
import dev.komu.ahwen.tx.Transaction

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

    override fun distinctValues(fieldName: String): Int =
        if (lhs.schema.hasField(fieldName)) lhs.distinctValues(fieldName) else rhs.distinctValues(fieldName)

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
