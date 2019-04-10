package dev.komu.ahwen.record

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.query.SqlInt
import dev.komu.ahwen.query.SqlString
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.SqlType
import java.io.Closeable

/**
 * A cursor for reading records described by [ti] from a file.
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

    fun getValue(column: ColumnName, type: SqlType) =
        rp.getValue(column, type)

    fun setValue(column: ColumnName, value: SqlValue) {
        rp.setValue(column, value)
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

fun RecordFile.getInt(column: ColumnName) =
    (getValue(column, SqlType.INTEGER) as SqlInt).value

fun RecordFile.getString(column: ColumnName) =
    (getValue(column, SqlType.VARCHAR) as SqlString).value

fun RecordFile.insertRow(vararg values: Pair<ColumnName, SqlValue>) {
    insert()
    for ((column, value) in values)
        setValue(column, value)
}
