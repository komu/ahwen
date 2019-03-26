package dev.komu.ahwen.log

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.FileManager
import dev.komu.ahwen.file.Page
import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE

class LogManager(
    private val fileManager: FileManager,
    private val logFile: String
) : Iterable<BasicLogRecord> {

    private val myPage = Page(fileManager)
    private lateinit var currentBlock: Block
    private var currentPos = 0

    init {
        val logSize = fileManager.size(logFile)
        if (logSize == 0) {
            appendNewBlock()
        } else {
            currentBlock = Block(logFile, logSize - 1)
            myPage.read(currentBlock)
            currentPos = lastRecordPosition * Int.SIZE_BYTES
        }
    }

    fun flush(lsn: Int) {
        if (lsn >= currentLSN)
            flush()
    }

    override fun iterator(): Iterator<BasicLogRecord> {
        flush()
        return LogIterator(fileManager, currentBlock)
    }

    @Synchronized
    fun append(rec: Array<Any>): Int {
        val recordSize = Int.SIZE_BYTES + rec.sumBy { size(it) }

        if (currentPos + recordSize >= BLOCK_SIZE) {
            flush()
            appendNewBlock()
        }

        for (obj in rec)
            appendVal(obj)

        finalizeRecord()

        return currentLSN
    }

    private fun appendVal(value: Any) {
        when (value) {
            is String ->
                myPage.setString(currentPos, value)
            is Int ->
                myPage.setInt(currentPos, value)
            else ->
                error("unexpected value $value of type ${value.javaClass}")
        }

        currentPos += size(value)
    }

    private fun size(value: Any): Int = when (value) {
        is String ->
            Page.strSize(value.length)
        is Int ->
            Int.SIZE_BYTES
        else ->
            error("unexpected value $value of type ${value.javaClass}")
    }

    private val currentLSN: Int
        get() = currentBlock.number

    private fun flush() {
        myPage.write(currentBlock)
    }

    private fun appendNewBlock() {
        lastRecordPosition = 0
        currentBlock = myPage.append(logFile)
        currentPos = Int.SIZE_BYTES
    }

    private fun finalizeRecord() {
        val lastPos = lastRecordPosition
        myPage.setInt(currentPos, lastPos)
        lastRecordPosition = currentPos
        currentPos += Int.SIZE_BYTES
    }

    private var lastRecordPosition: Int
        get() = myPage.getInt(LAST_POST)
        set(value) {
            myPage.setInt(LAST_POST, value)
        }

    companion object {
        private const val LAST_POST = 0
    }

    private class LogIterator(fileManager: FileManager, private var block: Block) : Iterator<BasicLogRecord> {

        private val page = Page(fileManager)
        private var currentRecord: Int

        init {
            page.read(block)
            currentRecord = page.getInt(LAST_POST)
        }

        override fun hasNext() = currentRecord > 0 || block.number > 0

        override fun next(): BasicLogRecord {
            if (currentRecord == 0)
                moveToNextBlock()
            currentRecord = page.getInt(currentRecord)
            return BasicLogRecord(page, currentRecord + Int.SIZE_BYTES)
        }

        private fun moveToNextBlock() {
            block = Block(block.filename, block.number - 1)
            page.read(block)
            currentRecord = page.getInt(LAST_POST)
        }
    }
}
