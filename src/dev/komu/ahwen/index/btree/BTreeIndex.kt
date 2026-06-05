package dev.komu.ahwen.index.btree

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.index.Index
import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.record.RID
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.IndexName
import dev.komu.ahwen.types.TableName
import kotlin.math.ln

/**
 * [Index] implemented as a B+ Tree.
 */
class BTreeIndex(
    indexName: IndexName,
    leafSchema: Schema,
    private val tx: Transaction
) : Index {

    private val leafTi: TableInfo = TableInfo(TableName("${indexName}leaf"), leafSchema)
    private val dirTi: TableInfo
    private val rootBlock: Block
    private var leaf: BTreeLeaf? = null

    init {
        if (tx.size(leafTi.fileName) == 0)
            tx.append(leafTi.fileName, BTreePageFormatter(leafTi, -1))

        val dirSchema = Schema {
            copyFieldFrom(BTreePage.COL_BLOCK, leafSchema)
            copyFieldFrom(BTreePage.COL_DATAVAL, leafSchema)
        }
        dirTi = TableInfo(TableName("${indexName}dir"), dirSchema)
        rootBlock = Block(dirTi.fileName, 0)
        if (tx.size(rootBlock.filename) == 0)
            tx.append(dirTi.fileName, BTreePageFormatter(dirTi, 0))

        val page = BTreePage(rootBlock, dirTi, tx)
        if (page.numRecs == 0) {
            val fieldType = dirSchema.type(BTreePage.COL_DATAVAL)
            page.insertDir(0, fieldType.minimumValue, 0)
        }
        page.close()
    }

    override fun beforeFirst(searchKey: SqlValue) {
        close()
        val root = BTreeDir(rootBlock, dirTi, tx)
        val blockNum = root.search(searchKey)
        root.close()
        val leafBlock = Block(leafTi.fileName, blockNum)
        leaf = BTreeLeaf(leafBlock, leafTi, searchKey, tx)
    }

    override fun next() =
        leaf!!.next()

    override val dataRid: RID
        get() = leaf!!.dataRid

    override fun insert(dataVal: SqlValue, dataRid: RID) {
        beforeFirst(dataVal)
        val e = leaf!!.insert(dataRid)
        leaf!!.close()
        if (e == null)
            return
        val root = BTreeDir(rootBlock, dirTi, tx)
        val e2 = root.insert(e)
        if (e2 != null)
            root.makeNewRoot(e2)
        root.close()
    }

    override fun delete(dataVal: SqlValue, dataRid: RID) {
        beforeFirst(dataVal)
        leaf!!.delete(dataRid)
        leaf!!.close()
    }

    override fun close() {
        leaf?.close()
    }

    companion object {

        fun searchCost(numBlocks: Int, rpb: Int): Int =
            1 + (ln(numBlocks.toDouble()) / ln(rpb.toDouble())).toInt()
    }
}
