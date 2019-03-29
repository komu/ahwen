package dev.komu.ahwen.query

import dev.komu.ahwen.metadata.MetadataManager
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction

class TablePlan(
    tableName: String,
    metadataManager: MetadataManager,
    private val tx: Transaction
) : Plan {

    private val ti = metadataManager.getTableInfo(tableName, tx)
    private val si = metadataManager.getStatInfo(tableName, ti, tx)

    override fun open(): Scan =
        TableScan(ti, tx)

    override val blocksAccessed: Int
        get() = si.numBlocks

    override val recordsOutput: Int
        get() = si.numRecords

    override fun distinctValues(fieldName: String): Int =
        si.distinctValues(fieldName)

    override val schema: Schema
        get() = ti.schema
}
