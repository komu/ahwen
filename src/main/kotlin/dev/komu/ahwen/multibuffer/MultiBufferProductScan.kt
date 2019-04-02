package dev.komu.ahwen.multibuffer

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.ProductScan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction

class MultiBufferProductScan(
    private val lhsScan: Scan,
    private val rhsTable: TableInfo,
    private val tx: Transaction,
    availableBuffers: Int
) : Scan {

    private val fileSize = tx.size(rhsTable.fileName)
    private val chunkSize = BufferNeeds.bestFactor(fileSize, availableBuffers)
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

    override fun getVal(fieldName: String): Constant =
        productScan.getVal(fieldName)

    override fun hasField(fieldName: String): Boolean =
        productScan.hasField(fieldName)

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
}
