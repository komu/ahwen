package dev.komu.ahwen.tx.recovery

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.file.Block
import dev.komu.ahwen.log.BasicLogRecord
import dev.komu.ahwen.log.LSN
import dev.komu.ahwen.log.LogManager

sealed class LogRecord {

    abstract fun writeToLog(logManager: LogManager): LSN
    abstract val op: Int
    abstract val txNumber: Int
    abstract fun undo(txnum: Int, bufferManager: BufferManager)

    companion object {
        const val CHECKPOINT = 0
        const val START = 1
        const val COMMIT = 2
        const val ROLLBACK = 3
        const val SETINT = 4
        const val SETSTRING = 5
    }
}

class CheckPointRecord : LogRecord() {

    override val op: Int
        get() = CHECKPOINT

    override val txNumber: Int
        get() = -1

    override fun undo(txnum: Int, bufferManager: BufferManager) {
    }

    override fun writeToLog(logManager: LogManager): LSN {
        return logManager.append(CHECKPOINT)
    }

    companion object {

        fun from(@Suppress("UNUSED_PARAMETER") rec: BasicLogRecord): CheckPointRecord {
            return CheckPointRecord()
        }
    }
}

class StartRecord(override val txNumber: Int) : LogRecord() {

    override val op: Int
        get() = START

    override fun undo(txnum: Int, bufferManager: BufferManager) {
    }

    override fun writeToLog(logManager: LogManager): LSN {
        return logManager.append(START, txNumber)
    }

    companion object {

        fun from(rec: BasicLogRecord): StartRecord {
            val tx = rec.nextInt()
            return StartRecord(tx)
        }
    }
}

class CommitRecord(override val txNumber: Int) : LogRecord() {

    override val op: Int
        get() = COMMIT

    override fun undo(txnum: Int, bufferManager: BufferManager) {
    }

    override fun writeToLog(logManager: LogManager): LSN {
        return logManager.append(COMMIT, txNumber)
    }

    companion object {

        fun from(rec: BasicLogRecord): CommitRecord {
            val tx = rec.nextInt()
            return CommitRecord(tx)
        }
    }
}

class RollbackRecord(override val txNumber: Int) : LogRecord() {

    override val op: Int
        get() = ROLLBACK

    override fun undo(txnum: Int, bufferManager: BufferManager) {
    }

    override fun writeToLog(logManager: LogManager): LSN {
        return logManager.append(ROLLBACK, txNumber)
    }

    companion object {

        fun from(rec: BasicLogRecord): RollbackRecord {
            val tx = rec.nextInt()
            return RollbackRecord(tx)
        }
    }
}

class SetIntRecord(
    override val txNumber: Int,
    private val block: Block,
    private val offset: Int,
    private val value: Int
    ) : LogRecord() {

    override val op: Int
        get() = SETINT

    override fun writeToLog(logManager: LogManager): LSN {
        return logManager.append(SETINT, txNumber, block.filename, block.number, offset, value)
    }

    override fun undo(txnum: Int, bufferManager: BufferManager) {
        val buffer = bufferManager.pin(block)
        buffer.setInt(offset, value, txNumber, LSN.undefined)
        bufferManager.unpin(buffer)
    }

    companion object {

        fun from(rec: BasicLogRecord): SetIntRecord {
            val tx = rec.nextInt()
            val filename = rec.nextString()
            val blockNum = rec.nextInt()
            val offset = rec.nextInt()
            val value = rec.nextInt()

            return SetIntRecord(tx, Block(filename, blockNum), offset, value)
        }
    }
}

class SetStringRecord(
    override val txNumber: Int,
    private val block: Block,
    private val offset: Int,
    private val value: String
    ) : LogRecord() {

    override val op: Int
        get() = SETSTRING

    override fun writeToLog(logManager: LogManager): LSN {
        return logManager.append(SETSTRING, txNumber, block.filename, block.number, offset, value)
    }

    override fun undo(txnum: Int, bufferManager: BufferManager) {
        val buffer = bufferManager.pin(block)
        buffer.setString(offset, value, txNumber, LSN.undefined)
        bufferManager.unpin(buffer)
    }

    companion object {

        fun from(rec: BasicLogRecord): SetStringRecord {
            val tx = rec.nextInt()
            val filename = rec.nextString()
            val blockNum = rec.nextInt()
            val offset = rec.nextInt()
            val value = rec.nextString()

            return SetStringRecord(tx, Block(filename, blockNum), offset, value)
        }
    }
}
