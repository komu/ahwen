package dev.komu.ahwen.log

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.FileManager
import dev.komu.ahwen.file.Page
import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.file.Page.Companion.INT_SIZE
import dev.komu.ahwen.file.Page.Companion.strSize
import dev.komu.ahwen.tx.TxNum
import dev.komu.ahwen.types.FileName
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Provides durable log functionality.
 *
 * The log provides three main functionalities:
 *
 * 1. Records can be _appended_ to log.
 * 2. Log can be _flushed_ to a certain point in time to make sure that records before that are written to disk.
 * 3. Records can be _iterated_ in reverse order.
 *
 * Log holds the last page in memory, accumulating changes to that and only writes it to disk when it
 * becomes full or a flush is requested.
 *
 * ## Layout of the log data
 *
 * Each log page starts with an int offset pointing to the start of last log record on the page.
 * Each log record starts with a pointer to the previous record, followed by the data of the record
 * itself. The log manager does not impose any structure for the data -- caller must be prepared
 * to handle that.
 *
 * The blocks are assumed to be contiguous, so there are no pointers to previous blocks: we can just
 * decrease the block number by one to get the previous block.
 *
 * ## Limitations
 *
 * The individual log entries may not be larger than a single page.
 */
class LogManager(
    private val fileManager: FileManager,
    private val logFile: FileName
) : Iterable<BasicLogRecord> {

    /** The last page's data */
    private val lastPage = Page(fileManager)

    /** Address of the last page in disk */
    private lateinit var currentBlock: Block

    /** Writing position in the last page */
    private var currentPos = 0

    private val lock = ReentrantLock()

    init {
        val lastBlock = fileManager.lastBlock(logFile)
        if (lastBlock == null) {
            appendNewBlock()
        } else {
            currentBlock = Block(logFile, lastBlock)
            lastPage.read(currentBlock)
            currentPos = lastRecordPosition * Int.SIZE_BYTES
        }
    }

    /**
     * Append a new record consisting of given components. Appending the record does not
     * guarantee that it will be flushed to disk immediately, but appending returns an [LSN]
     * which can later be passed to [flush] to make sure that all records up to (and including)
     * this record are flushed to disk.
     *
     * Note that passed in components are typed as [Any], but only [Int], [String] and [TxNum] values
     * are currently supported.
     */
    fun append(vararg values: Any): LSN {
        lock.withLock {
            val recordSize = INT_SIZE + values.sumBy { it.size }

            if (currentPos + recordSize >= BLOCK_SIZE) {
                flush()
                appendNewBlock()
            }

            for (value in values)
                appendValue(value)

            finalizeRecord()

            return currentLSN
        }
    }

    /**
     * Guarantees that log is flushed at least until the [append] call that returned given [LSN].
     */
    fun flush(lsn: LSN) {
        lock.withLock {
            if (lsn >= currentLSN)
                flush()
        }
    }

    /**
     * Flushes the whole log and returns an iterator that allows iterating all records of the log backwards.
     */
    override fun iterator(): Iterator<BasicLogRecord> {
        lock.withLock {
            flush()
            return LogIterator(fileManager, currentBlock)
        }
    }

    private val currentLSN: LSN
        get() = LSN(currentBlock.number)

    private fun appendValue(value: Any) {
        when (value) {
            is String ->
                lastPage.setString(currentPos, value)
            is TxNum ->
                lastPage.setInt(currentPos, value.txnum)
            is Int ->
                lastPage.setInt(currentPos, value)
            is FileName ->
                lastPage.setString(currentPos, value.value)
            else ->
                error("unexpected value $value of type ${value.javaClass}")
        }

        currentPos += value.size
    }

    private val Any.size: Int
        get() = when (this) {
            is String ->
                strSize(length)
            is FileName ->
                strSize(value.length)
            is Int, is TxNum ->
                INT_SIZE
            else ->
                error("unexpected value $this of type $javaClass")
        }

    private fun flush() {
        lastPage.write(currentBlock)
    }

    private fun appendNewBlock() {
        lastRecordPosition = 0
        currentBlock = lastPage.append(logFile)
        currentPos = INT_SIZE
    }

    private fun finalizeRecord() {
        val lastPos = lastRecordPosition
        lastPage.setInt(currentPos, lastPos)
        lastRecordPosition = currentPos
        currentPos += INT_SIZE
    }

    private var lastRecordPosition: Int
        get() = lastPage.getInt(LAST_POS_OFFSET)
        set(value) {
            lastPage.setInt(LAST_POS_OFFSET, value)
        }

    private class LogIterator(fileManager: FileManager, private var block: Block) : Iterator<BasicLogRecord> {

        private val page = Page(fileManager)
        private var currentRecord: Int

        init {
            page.read(block)
            currentRecord = page.getInt(LAST_POS_OFFSET)
        }

        override fun hasNext() = currentRecord > 0 || block.number > 0

        override fun next(): BasicLogRecord {
            if (currentRecord == 0)
                moveToNextBlock()

            currentRecord = page.getInt(currentRecord)
            return BasicLogRecord(page, currentRecord + INT_SIZE)
        }

        private fun moveToNextBlock() {
            block = Block(block.filename, block.number - 1)
            page.read(block)
            currentRecord = page.getInt(LAST_POS_OFFSET)
        }
    }

    companion object {

        /** The offset within block where the position of last block is stored */
        private const val LAST_POS_OFFSET = 0
    }
}
