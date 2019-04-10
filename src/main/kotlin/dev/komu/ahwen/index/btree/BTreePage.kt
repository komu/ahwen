package dev.komu.ahwen.index.btree

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.query.SqlInt
import dev.komu.ahwen.record.RID
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.*
import dev.komu.ahwen.types.ColumnName

/**
 * Low level class for B Tree page layout used by [BTreeDir] and [BTreeLeaf].
 */
class BTreePage(
    private var currentBlock: Block,
    private val ti: TableInfo,
    private val tx: Transaction
) {

    private val slotSize = ti.recordLength
    private var closed = false

    init {
        tx.pin(currentBlock)
    }

    fun findSlotBefore(searchKey: SqlValue): Int {
        var slot = 0
        while (slot < numRecs && getDataValue(slot) < searchKey)
            slot++
        return slot - 1
    }

    fun close() {
        // TODO: do we really need to guard against closing multiple times?
        if (!closed) {
            tx.unpin(currentBlock)
            closed = true
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

    fun getDataValue(slot: Int): SqlValue =
        getValue(slot, COL_DATAVAL)

    var flag: Int
        get() = tx.getInt(currentBlock, 0)
        set(value) {
            tx.setInt(currentBlock, 0, value)
        }

    private fun appendNew(flag: Int): Block =
        tx.append(ti.fileName, BTreePageFormatter(ti, flag))

    var numRecs: Int
        get() = tx.getInt(currentBlock, Int.SIZE_BYTES)
        private set(newValue) {
            tx.setInt(currentBlock, Int.SIZE_BYTES, newValue)
        }

    // Methods called only by BTreeDir

    fun getChildNum(slot: Int) =
        getInt(slot, COL_BLOCK)

    fun insertDir(slot: Int, value: SqlValue, blknum: Int) {
        insert(slot)
        setValue(slot, COL_DATAVAL, value)
        setInt(slot, COL_BLOCK, blknum)
    }

    // Methods called only by BTreeLeaf

    fun getDataRID(slot: Int): RID =
        RID(getInt(slot, COL_BLOCK), getInt(slot, COL_ID))

    fun insertLeaf(slot: Int, value: SqlValue, rid: RID) {
        insert(slot)
        setValue(slot, COL_DATAVAL, value)
        setInt(slot, COL_BLOCK, rid.blockNumber)
        setInt(slot, COL_ID, rid.id)
    }

    fun delete(slot: Int) {
        for (i in slot + 1 until numRecs)
            copyRecord(i, i - 1)
        numRecs--
    }

    // Private methods

    private fun getValue(slot: Int, fieldName: ColumnName): SqlValue {
        val pos = fieldPos(slot, fieldName)
        return tx.getValue(currentBlock, pos, ti.schema.type(fieldName))
    }

    private fun setValue(slot: Int, fieldName: ColumnName, value: SqlValue) {
        assert(value.type == ti.schema.type(fieldName))
        val pos = fieldPos(slot, fieldName)
        tx.setValue(currentBlock, pos, value)
    }

    private fun getInt(slot: Int, fieldName: ColumnName): Int =
        (getValue(slot, fieldName) as SqlInt).value

    private fun setInt(slot: Int, fieldName: ColumnName, value: Int) {
        setValue(slot, fieldName, SqlInt(value))
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
        for (column in schema.columns)
            setValue(to, column, getValue(from, column))
    }

    private fun transferRecords(slot: Int, dest: BTreePage) {
        val schema = ti.schema
        var destSlot = 0
        while (slot < numRecs) {
            dest.insert(destSlot)
            for (column in schema.columns)
                dest.setValue(destSlot, column, getValue(slot, column))
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
