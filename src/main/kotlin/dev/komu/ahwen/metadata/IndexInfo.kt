package dev.komu.ahwen.metadata

import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.index.Index
import dev.komu.ahwen.index.btree.BTreeIndex
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.SqlType

/**
 * Describes an index.
 *
 * Contains statistics about the index to guide planning decision. Also contains metadata
 * about the structure of the index so that it can be opened for scanning.
 */
class IndexInfo(
    private val indexName: String,
    tableName: String,
    private val fieldName: String,
    private val tx: Transaction,
    metadataManager: MetadataManager
) {

    private val ti = metadataManager.getTableInfo(tableName, tx)
    private val si = metadataManager.getStatInfo(tableName, ti, tx)

    fun open(): Index =
        BTreeIndex(indexName, schema(), tx)

    /**
     * Estimate how many blocks must be accessed to perform a load from index.
     */
    val blocksAccessed: Int
        get() {
            val rpb = BLOCK_SIZE / ti.recordLength
            val blockCount = si.numRecords / rpb
            return BTreeIndex.searchCost(blockCount, rpb)
        }

    /**
     * Estimate how many rows the output of index query will return.
     */
    val recordsOutput: Int
        get() = si.numRecords / si.distinctValues(fieldName)

    /**
     * Estimate how many distinct values index has for given field.
     */
    fun distinctValues(fieldName: String): Int =
        if (fieldName == this.fieldName)
            1
        else
            minOf(si.distinctValues(fieldName), recordsOutput)

    private fun schema() = Schema {
        intField("block")
        intField("id")
        val type = ti.schema.type(fieldName)
        when (type) {
            SqlType.INTEGER ->
                intField("dataval")
            SqlType.VARCHAR ->
                stringField("dataval", ti.schema.length(fieldName))
        }
    }

    override fun toString() = "[Index name=$indexName, field$fieldName]"
}
