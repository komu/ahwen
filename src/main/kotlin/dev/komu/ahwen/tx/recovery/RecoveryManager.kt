package dev.komu.ahwen.tx.recovery

import dev.komu.ahwen.buffer.Buffer
import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.file.Block
import dev.komu.ahwen.log.BasicLogRecord
import dev.komu.ahwen.log.LSN
import dev.komu.ahwen.log.LogManager
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.IntConstant
import dev.komu.ahwen.query.StringConstant
import dev.komu.ahwen.tx.TxNum

/**
 * Recovery manager is responsible for implementing commit, rollback and recovery from crashes.
 * The manager will intercept all modifications to data and writes a recovery log that can be used
 * to perform both rollback and recovery.
 *
 * The basic idea behind the recovery log is simple enough: whenever a change is made to a record,
 * we will first write a log entry that will tell us how the change can be undone. Only when the log
 * hits the disk will we write the actual change as well.
 *
 * If we need to recover from crash there are three possible states for each change:
 *
 * 1. Undo-entry was written to disk, but the changed buffer was not written. Executing the undo-entries
 *    will be a no-op, since they will just restore the data to the state that it already is.
 * 2. Both the undo-entry and the changed buffer were written, but the transaction was never committed.
 *    Executing the undo-entries will undo the changes to last committed state.
 * 3. Both the undo-entry and the changed buffer were written and the transaction was later committed.
 *    In this case we won't try to undo the changes.
 *
 * Rollback is similar to recovery: we'll lookup the log for all undo entries relating to the transaction
 * that we want to rollback and execute them.
 */
class RecoveryManager(
    private var txnum: TxNum,
    private val logManager: LogManager,
    private val bufferManager: BufferManager
) {

    init {
        val start = StartRecord(txnum)
        start.writeToLog(logManager)
    }

    /**
     * Commit a transaction, making it's changes permanent. If the method returns without
     * throwing exceptions, it's guaranteed that the data will be flushed to disk and is
     * recoverable even if the system crashes at some point.
     *
     * If system crashes during the commit, then after the recovery the data will be either
     * fully committed or fully rolled back, but not something in between.
     */
    fun commit() {
        bufferManager.flushAll(txnum)
        val record = CommitRecord(txnum)
        val lsn = record.writeToLog(logManager)
        logManager.flush(lsn)
    }

    /**
     * Rolls back a transaction, undoing all its changes.
     *
     * The nice thing about this method is that succeeds even when it fails. That is, if the
     * method returns normally, changes are guaranteed to be undone before returning. However,
     * if the system crashes before the method returns, the uncommitted changes will be undone
     * during recovery.
     */
    fun rollback() {
        doRollback()
        bufferManager.flushAll(txnum)
        val record = RollbackRecord(txnum)
        val lsn = record.writeToLog(logManager)
        logManager.flush(lsn)
    }

    /**
     * Performs recovery after system crash.
     *
     * Recovery will read the log backwards until last checkpoint and undo all changes relating
     * to transactions that are not committed. Since undoing changes several times is completely
     * safe, it doesn't matter if the system crashes in the middle of the recovery: we can simply
     * run the recovery again.
     */
    fun recover() {
        doRecover()
        bufferManager.flushAll(txnum)
        val record = CheckPointRecord()
        val lsn = record.writeToLog(logManager)
        logManager.flush(lsn)
    }

    fun setValue(buffer: Buffer, offset: Int, newValue: Constant): LSN {
        val oldValue = buffer.getValue(offset, newValue.type)
        val block = buffer.block ?: error("no block for buffer")
        return if (isTemporaryBlock(block)) {
            LSN.undefined
        } else {
            val record = createUndoRecord(block, offset, oldValue)
            record.writeToLog(logManager)
        }
    }

    private fun createUndoRecord(block: Block, offset: Int, oldValue: Constant): LogRecord = when (oldValue) {
        is IntConstant -> SetIntRecord(txnum, block, offset, oldValue.value)
        is StringConstant -> SetStringRecord(txnum, block, offset, oldValue.value)
    }

    private fun doRollback() {
        for (record in LogRecordIterator(logManager.iterator())) {
            if (record.txNumber == txnum) {
                if (record is StartRecord)
                    return
                record.undo(txnum, bufferManager)
            }
        }
    }

    private fun doRecover() {
        val committedTxs = mutableListOf<TxNum>()

        for (record in LogRecordIterator(logManager.iterator())) {
            when {
                record is CheckPointRecord ->
                    return
                record is CommitRecord ->
                    committedTxs.add(record.txNumber)
                record.txNumber !in committedTxs ->
                    record.undo(txnum, bufferManager)
            }
        }
    }

    private fun isTemporaryBlock(block: Block): Boolean =
        block.filename.isTemporary

    private class LogRecordIterator(private val iterator: Iterator<BasicLogRecord>) : Iterator<LogRecord> {
        override fun hasNext() = iterator.hasNext()
        override fun next() = LogRecord(iterator.next())
    }
}

