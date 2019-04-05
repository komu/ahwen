package dev.komu.ahwen.tx.recovery

import dev.komu.ahwen.buffer.Buffer
import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.file.Block
import dev.komu.ahwen.log.LSN
import dev.komu.ahwen.log.LogManager

class RecoveryManager(
    private var txnum: Int,
    private val logManager: LogManager,
    private val bufferManager: BufferManager
) {

    init {
        val start = StartRecord(txnum)
        start.writeToLog(logManager)
    }

    fun commit() {
        bufferManager.flushAll(txnum)
        val record = CommitRecord(txnum)
        val lsn = record.writeToLog(logManager)
        logManager.flush(lsn)
    }

    fun rollback() {
        doRollback()
        bufferManager.flushAll(txnum)
        val record = RollbackRecord(txnum)
        val lsn = record.writeToLog(logManager)
        logManager.flush(lsn)
    }

    fun recover() {
        doRecover()
        bufferManager.flushAll(txnum)
        val record = CheckPointRecord()
        val lsn = record.writeToLog(logManager)
        logManager.flush(lsn)
    }

    fun setInt(buffer: Buffer, offset: Int, @Suppress("UNUSED_PARAMETER") newValue: Int): LSN {
        val oldValue = buffer.getInt(offset)
        val block = buffer.block ?: error("no block for buffer")
        return if (isTemporaryBlock(block)) {
            LSN.undefined
        } else {
            val record = SetIntRecord(txnum, block, offset, oldValue)
            record.writeToLog(logManager)
        }
    }

    fun setString(buffer: Buffer, offset: Int, @Suppress("UNUSED_PARAMETER") newValue: String): LSN {
        val oldValue = buffer.getString(offset)
        val block = buffer.block ?: error("no block for buffer")
        return if (isTemporaryBlock(block)) {
            LSN.undefined
        } else {
            val record = SetStringRecord(txnum, block, offset, oldValue)
            record.writeToLog(logManager)
        }
    }

    private fun doRollback() {
        for (record in LogRecordIterator(logManager)) {
            if (record.txNumber == txnum) {
                if (record.op == LogRecord.START)
                    return
                record.undo(txnum, bufferManager)
            }
        }
    }

    private fun doRecover() {
        val committedTxs = mutableListOf<Int>()

        for (record in LogRecordIterator(logManager)) {
            if (record.op == LogRecord.CHECKPOINT)
                return

            if (record.op == LogRecord.COMMIT)
                committedTxs.add(record.txNumber)
            else if (record.txNumber !in committedTxs)
                record.undo(txnum, bufferManager)
        }
    }

    private fun isTemporaryBlock(block: Block): Boolean =
        block.filename.startsWith("temp")
}
