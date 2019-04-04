package dev.komu.ahwen.index.btree

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.IntConstant
import dev.komu.ahwen.query.StringConstant
import dev.komu.ahwen.record.RID
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.SqlType

class BTreePage(
    currentBlock: Block,
    private val ti: TableInfo,
    private val tx: Transaction
) {

    private var currentBlock: Block? = currentBlock
    val slotSize = ti.recordLength

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
        getVal(slot, "dataval")

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
        getInt(slot, "block")

    fun insertDir(slot: Int, value: Constant, blknum: Int) {
        insert(slot)
        setVal(slot, "dataval", value)
        setInt(slot, "block", blknum)
    }

    // Methods called only by BTreeLeaf

    fun getDataRID(slot: Int): RID =
        RID(getInt(slot, "block"), getInt(slot, "id"))

    fun insertLeaf(slot: Int, value: Constant, rid: RID) {
        insert(slot)
        setVal(slot, "dataval", value)
        setInt(slot, "block", rid.blockNumber)
        setInt(slot, "id", rid.id)
    }

    fun delete(slot: Int) {
        for (i in slot + 1 until numRecs)
            copyRecord(i, i - 1)
        numRecs--
    }

    // Private methods

    private fun getInt(slot: Int, fieldName: String): Int {
        val pos = fieldPos(slot, fieldName)
        return tx.getInt(currentBlock!!, pos)
    }

    private fun getString(slot: Int, fieldName: String): String {
        val pos = fieldPos(slot, fieldName)
        return tx.getString(currentBlock!!, pos)
    }

    private fun getVal(slot: Int, fieldName: String): Constant {
        val type = ti.schema.type(fieldName)
        return when (type) {
            SqlType.INTEGER -> IntConstant(getInt(slot, fieldName))
            SqlType.VARCHAR -> StringConstant(getString(slot, fieldName))
        }
    }

    private fun setInt(slot: Int, fieldName: String, value: Int) {
        val pos = fieldPos(slot, fieldName)
        tx.setInt(currentBlock!!, pos, value)
    }

    private fun setString(slot: Int, fieldName: String, value: String) {
        val pos = fieldPos(slot, fieldName)
        tx.setString(currentBlock!!, pos, value)
    }

    private fun setVal(slot: Int, fieldName: String, value: Constant) {
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

    private fun fieldPos(slot: Int, fieldName: String): Int =
        slotPos(slot) + ti.offset(fieldName)

    private fun slotPos(slot: Int): Int =
        Int.SIZE_BYTES + Int.SIZE_BYTES + (slot * slotSize)
}
