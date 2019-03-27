package dev.komu.ahwen.metadata

import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction
import java.sql.Types

class IndexInfo(
    private val indexName: String,
    tableName: String,
    private val fieldName: String,
    private val tx: Transaction,
    metadataManager: MetadataManager
) {

    private val ti = metadataManager.getTableInfo(tableName, tx)
    private val si = metadataManager.getStatInfo(tableName, ti, tx)

    fun open(): Index {
        val schema = schema()
        // return BTreeIndex(indexName, schema, tx)
        TODO()
    }

    val blocksAccessed: Int
        get() {
            val rpb = BLOCK_SIZE / ti.recordLength
            val blockCount = si.numRecords / rpb
            //return BTreeIndex.searchCost(blockCount, rpb)
            TODO()
        }

    val recordsOutput: Int
        get() = si.numRecords / si.distinctValues(fieldName)

    fun distinctValues(fieldName: String): Int =
        if (fieldName == this.fieldName)
            1
        else
            minOf(si.distinctValues(fieldName), recordsOutput)

    private fun schema(): Schema {
        val schema = Schema().apply {
            addIntField("block")
            addIntField("id")
            val type = ti.schema.type(fieldName)
            when (type) {
                Types.INTEGER ->
                    addIntField("dataval")
                Types.VARCHAR ->
                    addStringField("dataval", ti.schema.length(fieldName))
                else ->
                    error("invalid type $type")
            }
        }

        return schema
    }
}
