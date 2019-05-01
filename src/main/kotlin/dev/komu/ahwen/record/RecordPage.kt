package dev.komu.ahwen.record

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.tx.getInt
import dev.komu.ahwen.tx.setInt
import dev.komu.ahwen.types.ColumnName

/**
 * A cursor for reading records described by [ti] from a block.
 */
class RecordPage(
    private val block: Block,
    private val ti: TableInfo,
    private val tx: Transaction
) {

    private val slotSize = ti.recordLength + Int.SIZE_BYTES
    var currentId = -1
        private set

    init {
        tx.pin(block)
    }

    fun close() {
        tx.unpin(block)
    }

    fun next() = searchFor(IN_USE)

    operator fun get(column: ColumnName) =
        tx.getValue(block, columnPosition(column), ti.schema.type(column))

    operator fun set(column: ColumnName, value: SqlValue) {
        assert(ti.schema.type(column) == value.type)
        tx.setValue(block, columnPosition(column), value)
    }

    fun delete() {
        tx.setInt(block, currentPos, EMPTY)
    }

    fun insert(): Boolean {
        this.currentId = -1
        val found = searchFor(EMPTY)
        if (found)
            tx.setInt(block, currentPos, IN_USE)
        return found
    }

    fun moveToId(id: Int) {
        this.currentId = id
    }

    private val currentPos: Int
        get() = currentId * slotSize

    private fun columnPosition(name: ColumnName): Int {
        val offset = Int.SIZE_BYTES + ti.offset(name)
        return currentPos + offset
    }

    private val isValidSlot: Boolean
        get() = currentPos + slotSize <= BLOCK_SIZE

    private fun searchFor(flag: Int): Boolean {
        currentId++

        while (isValidSlot) {
            if (tx.getInt(block, currentPos) == flag)
                return true
            currentId++
        }

        return false
    }

    companion object {
        const val EMPTY = 0
        const val IN_USE = 1
    }
}
