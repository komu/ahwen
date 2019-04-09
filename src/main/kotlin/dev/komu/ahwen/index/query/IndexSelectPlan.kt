package dev.komu.ahwen.index.query

import dev.komu.ahwen.metadata.IndexInfo
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.TablePlan
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.types.ColumnName

class IndexSelectPlan(
    private val plan: TablePlan,
    private val indexInfo: IndexInfo,
    private val value: Constant
) : Plan {

    override fun open(): Scan =
        IndexSelectScan(indexInfo.open(), value, plan.open())

    override val blocksAccessed: Int
        get() = indexInfo.blocksAccessed + recordsOutput

    override val recordsOutput: Int
        get() = indexInfo.recordsOutput

    override fun distinctValues(column: ColumnName): Int =
        indexInfo.distinctValues(column)

    override val schema: Schema
        get() = plan.schema
}
