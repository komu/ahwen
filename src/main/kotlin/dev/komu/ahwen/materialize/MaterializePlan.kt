package dev.komu.ahwen.materialize

import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import kotlin.math.ceil

class MaterializePlan(private val srcPlan: Plan, private val tx: Transaction) : Plan by srcPlan {

    override fun open(): Scan {
        val schema = srcPlan.schema
        val temp = TempTable(schema, tx)
        val src = srcPlan.open()
        val dest = temp.open()
        while (src.next()) {
            dest.insert()
            for (field in schema.fields)
                dest.setVal(field, src.getVal(field))
        }
        src.close()
        dest.beforeFirst()
        return dest
    }

    override val blocksAccessed: Int
        get() {
            val ti = TableInfo("", srcPlan.schema)
            val rpb = (BLOCK_SIZE / ti.recordLength).toDouble()
            return ceil(srcPlan.recordsOutput / rpb).toInt()
        }
}
