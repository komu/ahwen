package dev.komu.ahwen.query.materialize

import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.copyFrom
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.TableName
import kotlin.math.ceil

class MaterializePlan(private val srcPlan: Plan, private val tx: Transaction) : Plan by srcPlan {

    override fun open(): Scan {
        val schema = srcPlan.schema
        val temp = TempTable(schema, tx)
        val dest = temp.open()
        dest.copyFrom(srcPlan, schema)
        dest.beforeFirst()
        return dest
    }

    override val blocksAccessed: Int
        get() {
            val ti = TableInfo(TableName.DUMMY, srcPlan.schema)
            val rpb = (BLOCK_SIZE / ti.recordLength).toDouble()
            return ceil(srcPlan.recordsOutput / rpb).toInt()
        }
}
