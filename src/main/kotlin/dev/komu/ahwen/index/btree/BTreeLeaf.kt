package dev.komu.ahwen.index.btree

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.record.RID
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction

class BTreeLeaf(
    block: Block,
    private val ti: TableInfo,
    private val searchKey: Constant,
    private val tx: Transaction
) {

    private var contents = BTreePage(block, ti, tx)
    private var currentSlot = contents.findSlotBefore(searchKey)

    fun close() {
        contents.close()
    }

    fun next(): Boolean {
        currentSlot++
        return when {
            currentSlot >= contents.numRecs -> tryOverflow()
            contents.getDataValue(currentSlot) == searchKey -> true
            else -> tryOverflow()
        }
    }

    val dataRid: RID
        get() = contents.getDataRID(currentSlot)

    fun delete(dataRid: RID) {
        while (next()) {
            if (this.dataRid == dataRid) {
                contents.delete(currentSlot)
                return
            }
        }
    }

    fun insert(dataRid: RID): DirEntry? {
        if (contents.flag >= 0 && contents.getDataValue(0) > searchKey) {
            val firstVal = contents.getDataValue(0)
            val newBlock = contents.split(0, contents.flag)
            currentSlot = 0
            contents.flag = -1
            contents.insertLeaf(currentSlot, searchKey, dataRid)
            return DirEntry(firstVal, newBlock.number)
        }

        currentSlot++
        contents.insertLeaf(currentSlot, searchKey, dataRid)
        if (!contents.isFull)
            return null

        val firstKey = contents.getDataValue(0)
        val lastKey = contents.getDataValue(contents.numRecs - 1)
        if (lastKey == firstKey) {
            val newBlock = contents.split(1, contents.flag)
            contents.flag = newBlock.number
            return null
        } else {
            var splitPos = contents.numRecs / 2
            var splitKey = contents.getDataValue(splitPos)
            if (splitKey == firstKey) {
                while (contents.getDataValue(splitPos) == splitKey)
                    splitPos++
                splitKey = contents.getDataValue(splitPos)
            } else {
                while (contents.getDataValue(splitPos - 1) == splitKey)
                    splitPos--
            }
            val newBlock = contents.split(splitPos, -1)
            return DirEntry(splitKey, newBlock.number)
        }
    }

    private fun tryOverflow(): Boolean {
        val firstKey = contents.getDataValue(0)
        val flag = contents.flag
        if (searchKey != firstKey || flag < 0)
            return false

        contents.close()
        val nextBlock = Block(ti.fileName, flag)
        contents = BTreePage(nextBlock, ti, tx)
        currentSlot = 0
        return true
    }
}
