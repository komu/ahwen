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
            contents.getDataVal(currentSlot) == searchKey -> true
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
        if (contents.flag >= 0 && contents.getDataVal(0) > searchKey) {
            val firstVal = contents.getDataVal(0)
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

        val firstKey = contents.getDataVal(0)
        val lastKey = contents.getDataVal(contents.numRecs - 1)
        if (lastKey == firstKey) {
            val newBlock = contents.split(1, contents.flag)
            contents.flag = newBlock.number
            return null
        } else {
            var splitPos = contents.numRecs / 2
            var splitKey = contents.getDataVal(splitPos)
            if (splitKey == firstKey) {
                while (contents.getDataVal(splitPos) == splitKey)
                    splitPos++
                splitKey = contents.getDataVal(splitPos)
            } else {
                while (contents.getDataVal(splitPos - 1) == splitKey)
                    splitPos--
            }
            val newBlock = contents.split(splitPos, -1)
            return DirEntry(splitKey, newBlock.number)
        }
    }

    private fun tryOverflow(): Boolean {
        val firstKey = contents.getDataVal(0)
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
