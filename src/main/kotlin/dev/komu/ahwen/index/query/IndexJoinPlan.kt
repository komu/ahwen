package dev.komu.ahwen.index.query

import dev.komu.ahwen.metadata.IndexInfo
import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.TablePlan

class IndexJoinPlan(
    private val p1: Plan,
    private val p2: TablePlan,
    private val indexInfo: IndexInfo,
    private val joinField: String
) : Plan {

    override val schema = p1.schema + p2.schema

    override fun open(): Scan =
        IndexJoinScan(p1.open(), indexInfo.open(), joinField, p2.open())

    override val blocksAccessed: Int
        get() = p1.blocksAccessed + (p1.recordsOutput * indexInfo.blocksAccessed) + recordsOutput

    override val recordsOutput: Int
        get() = p1.recordsOutput * indexInfo.recordsOutput

    override fun distinctValues(fieldName: String): Int =
        if (p1.schema.hasField(fieldName)) p1.distinctValues(fieldName) else p2.distinctValues(fieldName)

    override fun toString() = "[IndexJoinPlan p1=$p1, p2=$p2, index=$indexInfo, joinField=$joinField]"
}
