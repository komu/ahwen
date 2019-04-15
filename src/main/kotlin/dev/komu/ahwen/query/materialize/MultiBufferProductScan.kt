package dev.komu.ahwen.query.materialize

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.query.ProductScan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.query.bestFactor
import dev.komu.ahwen.record.RecordPage
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName

/**
 * Runtime implementation of [MultiBufferProductPlan].
 */
class MultiBufferProductScan(
    private val lhsScan: Scan,
    private val rhsTable: TableInfo,
    private val tx: Transaction,
    availableBuffers: Int
) : Scan {

    private val fileSize = tx.size(rhsTable.fileName)
    private val chunkSize = bestFactor(fileSize, availableBuffers)
    private var nextBlockNum = 0
    private var rhsScan: Scan? = null
    private lateinit var productScan: Scan

    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        nextBlockNum = 0
        useNextChunk()
    }

    override fun next(): Boolean {
        while (!productScan.next())
            if (!useNextChunk())
                return false
        return true
    }

    override fun close() {
        productScan.close()
    }

    override fun get(column: ColumnName): SqlValue =
        productScan[column]

    override fun contains(column: ColumnName): Boolean =
        column in productScan

    private fun useNextChunk(): Boolean {
        rhsScan?.close()
        if (nextBlockNum >= fileSize)
            return false

        var end = nextBlockNum + chunkSize - 1
        if (end >= fileSize)
            end = fileSize - 1

        rhsScan = ChunkScan(rhsTable, nextBlockNum, end, tx)
        lhsScan.beforeFirst()
        productScan = ProductScan(lhsScan, rhsScan!!)
        nextBlockNum = end + 1
        return true
    }

    private class ChunkScan(
        ti: TableInfo,
        private val startBlockNum: Int,
        private val endBlockNum: Int,
        private val tx: Transaction
    ) : Scan {

        private val schema = ti.schema

        private val pages: List<RecordPage> = (startBlockNum..endBlockNum).map { blockNum ->
            RecordPage(Block(ti.fileName, blockNum), ti, tx)
        }
        private var current = startBlockNum
        private var rp: RecordPage = pages.first()

        init {
            rp.moveToId(-1)
        }

        override fun beforeFirst() {
            moveToBlock(startBlockNum)
        }

        override fun next(): Boolean {
            while (true) {
                if (rp.next())
                    return true
                if (current == endBlockNum)
                    return false
                moveToBlock(current + 1)
            }
        }

        override fun get(column: ColumnName): SqlValue =
            rp[column]

        override fun contains(column: ColumnName): Boolean =
            column in schema

        override fun close() {
            pages.forEach(RecordPage::close)
        }

        private fun moveToBlock(block: Int) {
            current = block
            rp = pages[current - startBlockNum]
            rp.moveToId(-1)
        }
    }
}
