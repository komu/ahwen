package dev.komu.ahwen.query

import dev.komu.ahwen.metadata.MetadataManager
import dev.komu.ahwen.record.RID
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.TableName

/**
 * A plan for reading data from a table.
 *
 * If the resulting [TableScan] is executed as it is, it will perform a full table scan,
 * but it also offers methods to navigate directly onto certain row by [RID]. This is
 * useful with indices that store these row ids.
 */
class TablePlan(
    tableName: TableName,
    metadataManager: MetadataManager,
    private val tx: Transaction
) : Plan {

    private val ti = metadataManager.getTableInfo(tableName, tx)
    private val si = metadataManager.getStatInfo(tableName, ti, tx)

    override fun open(): TableScan =
        TableScan(ti, tx)

    override val blocksAccessed: Int
        get() = si.numBlocks

    override val recordsOutput: Int
        get() = si.numRecords

    override fun distinctValues(column: ColumnName): Int =
        si.distinctValues(column)

    override val schema: Schema
        get() = ti.schema

    override fun toString() = "[TablePlan name=${ti.tableName}]"
}
