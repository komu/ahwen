package dev.komu.ahwen.query

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.record.RecordPage
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.SqlType.INTEGER
import dev.komu.ahwen.types.SqlType.VARCHAR

class ChunkScan(
    ti: TableInfo,
    private val startBlockNum: Int,
    private val endBlockNum: Int,
    private val tx: Transaction
) : Scan {

    private val schema = ti.schema

    private val pages: List<RecordPage> = (startBlockNum..endBlockNum).map { blockNum ->
        RecordPage(Block(ti.fileName, blockNum), ti, tx)
    }
    private var current = startBlockNum
    private var rp: RecordPage = pages.first()

    init {
        rp.moveToId(-1)
    }

    override fun beforeFirst() {
        moveToBlock(startBlockNum)
    }

    override fun next(): Boolean {
        while (true) {
            if (rp.next())
                return true
            if (current == endBlockNum)
                return false
            moveToBlock(current + 1)
        }
    }

    override fun get(column: ColumnName): Constant = when (schema.type(column)) {
        INTEGER -> IntConstant(rp.getInt(column))
        VARCHAR -> StringConstant(rp.getString(column))
    }

    override fun contains(column: ColumnName): Boolean =
        column in schema

    override fun close() {
        pages.forEach(RecordPage::close)
    }

    private fun moveToBlock(block: Int) {
        current = block
        rp = pages[current - startBlockNum]
        rp.moveToId(-1)
    }
}
