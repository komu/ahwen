package dev.komu.ahwen.record

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.tx.Transaction
import java.io.Closeable

/**
 * A cursor into reading records described by [ti] from a file.
 */
class RecordFile(private val ti: TableInfo, private val tx: Transaction) : Closeable {

    private val filename = ti.fileName
    private var rp: RecordPage
    private var currentBlockNum = 0

    init {
        if (tx.size(filename) == 0)
            appendBlock()
        rp = RecordPage(Block(filename, currentBlockNum), ti, tx)
    }

    override fun close() {
        rp.close()
    }

    fun beforeFirst() {
        moveTo(0)
    }

    fun next(): Boolean {
        while (true) {
            if (rp.next())
                return true

            if (atLastBlock)
                return false

            moveTo(currentBlockNum + 1)
        }
    }

    fun getInt(field: String) =
        rp.getInt(field)

    fun getString(field: String) =
        rp.getString(field)

    fun setInt(field: String, value: Int) {
        rp.setInt(field, value)
    }

    fun setString(field: String, value: String) {
        rp.setString(field, value)
    }

    fun delete() {
        rp.delete()
    }

    fun insert() {
        while (!rp.insert()) {
            if (atLastBlock)
                appendBlock()

            moveTo(currentBlockNum + 1)
        }
    }

    fun moveToRid(rid: RID) {
        moveTo(rid.blockNumber)
        rp.moveToId(rid.id)
    }

    val currentRid: RID
        get() = RID(currentBlockNum, rp.currentId)

    private fun moveTo(b: Int) {
        rp.close()

        currentBlockNum = b
        rp = RecordPage(Block(filename, currentBlockNum), ti, tx)
    }

    private val atLastBlock: Boolean
        get() = currentBlockNum == tx.size(filename) - 1

    private fun appendBlock() {
        tx.append(filename, RecordFormatter(ti))
    }
}

inline fun RecordFile.forEach(func: () -> Unit) {
    while (next())
        func()
}
