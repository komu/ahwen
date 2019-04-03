package dev.komu.ahwen.multibuffer

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.IntConstant
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.StringConstant
import dev.komu.ahwen.record.RecordPage
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import java.sql.Types

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

    override fun getVal(fieldName: String): Constant {
        val type = schema.type(fieldName)
        return when (type) {
            Types.INTEGER -> IntConstant(rp.getInt(fieldName))
            Types.VARCHAR -> StringConstant(rp.getString(fieldName))
            else -> error("unknown type: $type")
        }
    }

    override fun hasField(fieldName: String): Boolean =
        schema.hasField(fieldName)

    override fun close() {
        pages.forEach(RecordPage::close)
    }

    private fun moveToBlock(block: Int) {
        current = block
        rp = pages[current - startBlockNum]
        rp.moveToId(-1)
    }
}