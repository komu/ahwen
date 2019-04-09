package dev.komu.ahwen.index.btree

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.IntConstant
import dev.komu.ahwen.query.StringConstant
import dev.komu.ahwen.record.RID
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.SqlType

class BTreePage(
    currentBlock: Block,
    private val ti: TableInfo,
    private val tx: Transaction
) {

    private var currentBlock: Block? = currentBlock
    private val slotSize = ti.recordLength

    init {
        tx.pin(currentBlock)
    }

    fun findSlotBefore(searchKey: Constant): Int {
        var slot = 0
        while (slot < numRecs && getDataVal(slot) < searchKey)
            slot++
        return slot - 1
    }

    fun close() {
        if (currentBlock != null) {
            tx.unpin(currentBlock!!)
            currentBlock = null
        }
    }

    val isFull: Boolean
        get() = slotPos(numRecs + 1) >= BLOCK_SIZE

    fun split(splitpos: Int, flag: Int): Block {
        val newBlock = appendNew(flag)
        val newPage = BTreePage(newBlock, ti, tx)
        transferRecords(splitpos, newPage)
        newPage.flag = flag
        newPage.close()
        return newBlock
    }

    fun getDataVal(slot: Int): Constant =
        getVal(slot, COL_DATAVAL)

    var flag: Int
        get() = tx.getInt(currentBlock!!, 0)
        set(value) {
            tx.setInt(currentBlock!!, 0, value)
        }

    fun appendNew(flag: Int): Block =
        tx.append(ti.fileName, BTreePageFormatter(ti, flag))

    var numRecs: Int
        get() = tx.getInt(currentBlock!!, Int.SIZE_BYTES)
        private set(newValue) {
            tx.setInt(currentBlock!!, Int.SIZE_BYTES, newValue)
        }

    // Methods called only by BTreeDir

    fun getChildNum(slot: Int) =
        getInt(slot, COL_BLOCK)

    fun insertDir(slot: Int, value: Constant, blknum: Int) {
        insert(slot)
        setVal(slot, COL_DATAVAL, value)
        setInt(slot, COL_BLOCK, blknum)
    }

    // Methods called only by BTreeLeaf

    fun getDataRID(slot: Int): RID =
        RID(getInt(slot, COL_BLOCK), getInt(slot, COL_ID))

    fun insertLeaf(slot: Int, value: Constant, rid: RID) {
        insert(slot)
        setVal(slot, COL_DATAVAL, value)
        setInt(slot, COL_BLOCK, rid.blockNumber)
        setInt(slot, COL_ID, rid.id)
    }

    fun delete(slot: Int) {
        for (i in slot + 1 until numRecs)
            copyRecord(i, i - 1)
        numRecs--
    }

    // Private methods

    private fun getInt(slot: Int, fieldName: ColumnName): Int {
        val pos = fieldPos(slot, fieldName)
        return tx.getInt(currentBlock!!, pos)
    }

    private fun getString(slot: Int, fieldName: ColumnName): String {
        val pos = fieldPos(slot, fieldName)
        return tx.getString(currentBlock!!, pos)
    }

    private fun getVal(slot: Int, fieldName: ColumnName): Constant {
        val type = ti.schema.type(fieldName)
        return when (type) {
            SqlType.INTEGER -> IntConstant(getInt(slot, fieldName))
            SqlType.VARCHAR -> StringConstant(getString(slot, fieldName))
        }
    }

    private fun setInt(slot: Int, fieldName: ColumnName, value: Int) {
        val pos = fieldPos(slot, fieldName)
        tx.setInt(currentBlock!!, pos, value)
    }

    private fun setString(slot: Int, fieldName: ColumnName, value: String) {
        val pos = fieldPos(slot, fieldName)
        tx.setString(currentBlock!!, pos, value)
    }

    private fun setVal(slot: Int, fieldName: ColumnName, value: Constant) {
        val type = ti.schema.type(fieldName)
        when (type) {
            SqlType.INTEGER -> setInt(slot, fieldName, value.value as Int)
            SqlType.VARCHAR -> setString(slot, fieldName, value.value as String)
        }
    }

    private fun insert(slot: Int) {
        var i = numRecs
        while (i > slot) {
            copyRecord(i - 1, i)
            i--
        }
        numRecs++
    }

    private fun copyRecord(from: Int, to: Int) {
        val schema = ti.schema
        for (field in schema.fields)
            setVal(to, field, getVal(from, field))
    }

    private fun transferRecords(slot: Int, dest: BTreePage) {
        val schema = ti.schema
        var destSlot = 0
        while (slot < numRecs) {
            dest.insert(destSlot)
            for (fieldName in schema.fields)
                dest.setVal(destSlot, fieldName, getVal(slot, fieldName))
            delete(slot)
            destSlot++
        }
    }

    private fun fieldPos(slot: Int, fieldName: ColumnName): Int =
        slotPos(slot) + ti.offset(fieldName)

    private fun slotPos(slot: Int): Int =
        Int.SIZE_BYTES + Int.SIZE_BYTES + (slot * slotSize)

    companion object {

        val COL_BLOCK = ColumnName("block")
        val COL_DATAVAL = ColumnName("dataval")
        val COL_ID = ColumnName("id")
    }
}
