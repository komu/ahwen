package dev.komu.ahwen.index.btree

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction

class BTreeDir(
    block: Block,
    private val ti: TableInfo,
    private val tx: Transaction
) {

    private val fileName = ti.fileName
    private var contents = BTreePage(block, ti, tx)

    fun close() {
        contents.close()
    }

    fun search(searchKey: Constant): Int {
        var childBlock = findChildBlock(searchKey)
        while (contents.flag > 0) {
            contents.close()
            contents = BTreePage(childBlock, ti, tx)
            childBlock = findChildBlock(searchKey)
        }
        return childBlock.number
    }

    fun makeNewRoot(e: DirEntry) {
        val firstVal = contents.getDataVal(0)
        val level = contents.flag
        val newBlock = contents.split(0, level)
        val oldRoot = DirEntry(firstVal, newBlock.number)
        insertEntry(oldRoot)
        insertEntry(e)
        contents.flag = level + 1
    }

    fun insert(e: DirEntry): DirEntry? {
        if (contents.flag == 0)
            return insertEntry(e)

        val childBlock = findChildBlock(e.dataval)
        val child = BTreeDir(childBlock, ti, tx)
        val myentry = child.insert(e)
        child.close()
        return if (myentry != null) insertEntry(myentry) else null
    }

    private fun insertEntry(e: DirEntry): DirEntry? {
        val newSlot = 1 + contents.findSlotBefore(e.dataval)
        contents.insertDir(newSlot, e.dataval, e.blocknum)
        if (!contents.isFull)
            return null

        val level = contents.flag
        val splitPos = contents.numRecs / 2
        val splitVal = contents.getDataVal(splitPos)
        val newBlock = contents.split(splitPos, level)

        return DirEntry(splitVal, newBlock.number)
    }

    private fun findChildBlock(searchKey: Constant): Block {
        var slot = contents.findSlotBefore(searchKey)
        if (contents.getDataVal(slot + 1) == searchKey)
            slot++
        val blockNum = contents.getChildNum(slot)
        return Block(fileName, blockNum)
    }
}
