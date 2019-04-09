package dev.komu.ahwen.tx.recovery

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.file.Block
import dev.komu.ahwen.log.BasicLogRecord
import dev.komu.ahwen.log.LSN
import dev.komu.ahwen.log.LogManager
import dev.komu.ahwen.tx.TxNum
import dev.komu.ahwen.types.FileName

/**
 * Base class for different records of the recovery log.
 */
sealed class LogRecord {

    /**
     * Identifier for the transaction that this change belongs to or
     * `null` if the record is not logically related to any transaction.
     */
    abstract val txNumber: TxNum?

    /**
     * Serializes this record to given log-manager.
     */
    abstract fun writeToLog(logManager: LogManager): LSN

    /**
     * Undoes the changes represented by this log-record.
     */
    open fun undo(txnum: TxNum, bufferManager: BufferManager) {
    }

    companion object {

        const val CHECKPOINT = 0
        const val START = 1
        const val COMMIT = 2
        const val ROLLBACK = 3
        const val SETINT = 4
        const val SETSTRING = 5

        /**
         * Parse a [LogRecord] from a [BasicLogRecord]. Dual of [writeToLog].
         */
        operator fun invoke(record: BasicLogRecord): LogRecord {
            val type = record.nextInt()
            return when (type) {
                CHECKPOINT -> CheckPointRecord.from(record)
                START -> StartRecord.from(record)
                COMMIT -> CommitRecord.from(record)
                ROLLBACK -> RollbackRecord.from(record)
                SETINT -> SetIntRecord.from(record)
                SETSTRING -> SetStringRecord.from(record)
                else -> error("invalid record type: $type")
            }
        }
    }
}

/**
 * Checkpoints represent a point in time where there are no running transactions and
 * all changes have been flushed to disk. When performing recovery and undoing uncommitted
 * changes, we can stop once we reach a checkpoint.
 */
class CheckPointRecord : LogRecord() {

    override val txNumber: TxNum?
        get() = null

    override fun writeToLog(logManager: LogManager): LSN =
        logManager.append(CHECKPOINT)

    companion object {

        fun from(@Suppress("UNUSED_PARAMETER") rec: BasicLogRecord): CheckPointRecord {
            return CheckPointRecord()
        }
    }
}

/**
 * Marks the beginning of a transaction.
 */
class StartRecord(override val txNumber: TxNum) : LogRecord() {

    override fun writeToLog(logManager: LogManager): LSN =
        logManager.append(START, txNumber)

    companion object {

        fun from(rec: BasicLogRecord): StartRecord {
            val tx = TxNum(rec.nextInt())
            return StartRecord(tx)
        }
    }
}

/**
 * Marks a transaction as committed.
 */
class CommitRecord(override val txNumber: TxNum) : LogRecord() {

    override fun writeToLog(logManager: LogManager): LSN =
        logManager.append(COMMIT, txNumber)

    companion object {

        fun from(rec: BasicLogRecord): CommitRecord {
            val tx = TxNum(rec.nextInt())
            return CommitRecord(tx)
        }
    }
}

/**
 * Marks a transaction as rolled back.
 */
class RollbackRecord(override val txNumber: TxNum) : LogRecord() {

    override fun writeToLog(logManager: LogManager): LSN =
        logManager.append(ROLLBACK, txNumber)

    companion object {

        fun from(rec: BasicLogRecord): RollbackRecord {
            val tx = TxNum(rec.nextInt())
            return RollbackRecord(tx)
        }
    }
}

/**
 * Undo record for changing an int.
 */
class SetIntRecord(
    override val txNumber: TxNum,
    private val block: Block,
    private val offset: Int,
    private val oldValue: Int
    ) : LogRecord() {

    override fun writeToLog(logManager: LogManager): LSN =
        logManager.append(SETINT, txNumber, block.filename, block.number, offset, oldValue)

    override fun undo(txnum: TxNum, bufferManager: BufferManager) {
        val buffer = bufferManager.pin(block)
        buffer.setInt(offset, oldValue, txnum, LSN.undefined)
        bufferManager.unpin(buffer)
    }

    companion object {

        fun from(rec: BasicLogRecord): SetIntRecord {
            val tx = TxNum(rec.nextInt())
            val filename = FileName(rec.nextString())
            val blockNum = rec.nextInt()
            val offset = rec.nextInt()
            val value = rec.nextInt()

            return SetIntRecord(tx, Block(filename, blockNum), offset, value)
        }
    }
}

/**
 * Undo record for changing a string.
 */
class SetStringRecord(
    override val txNumber: TxNum,
    private val block: Block,
    private val offset: Int,
    private val odValue: String
    ) : LogRecord() {

    override fun writeToLog(logManager: LogManager): LSN =
        logManager.append(SETSTRING, txNumber, block.filename, block.number, offset, odValue)

    override fun undo(txnum: TxNum, bufferManager: BufferManager) {
        val buffer = bufferManager.pin(block)
        buffer.setString(offset, odValue, txnum, LSN.undefined)
        bufferManager.unpin(buffer)
    }

    companion object {

        fun from(rec: BasicLogRecord): SetStringRecord {
            val tx = TxNum(rec.nextInt())
            val filename = FileName(rec.nextString())
            val blockNum = rec.nextInt()
            val offset = rec.nextInt()
            val value = rec.nextString()

            return SetStringRecord(tx, Block(filename, blockNum), offset, value)
        }
    }
}
