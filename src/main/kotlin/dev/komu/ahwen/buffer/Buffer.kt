package dev.komu.ahwen.buffer

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.FileManager
import dev.komu.ahwen.file.Page
import dev.komu.ahwen.log.LSN
import dev.komu.ahwen.log.LogManager

class Buffer(fileManager: FileManager, private val logManager: LogManager) {

    private val contents = Page(fileManager)
    var block: Block? = null
        private set
    private var pins = 0
    private var modifiedBy: Int? = null
    private var logSequenceNumber = LSN.zero

    fun getInt(offset: Int): Int =
        contents.getInt(offset)

    fun getString(offset: Int): String =
        contents.getString(offset)

    fun setInt(offset: Int, value: Int, txnum: Int, lsn: LSN) {
        modifiedBy = txnum
        if (lsn >= LSN.zero)
            logSequenceNumber = lsn
        contents.setInt(offset, value)
    }

    fun setString(offset: Int, value: String, txnum: Int, lsn: LSN) {
        modifiedBy = txnum
        if (lsn >= LSN.zero)
            logSequenceNumber = lsn
        contents.setString(offset, value)
    }

    fun flush() {
        if (modifiedBy != null) {
            logManager.flush(logSequenceNumber)
            contents.write(block ?: error("no block"))
            modifiedBy = null
        }
    }

    fun pin() {
        pins++
    }

    fun unpin() {
        pins--
    }

    val isPinned: Boolean
        get() = pins > 0

    fun isModifiedBy(txnum: Int) =
        txnum == modifiedBy

    fun assignToBlock(block: Block) {
        flush()
        this.block = block
        contents.read(block)
        pins = 0
    }

    fun assignToNew(fileName: String, formatter: PageFormatter) {
        flush()
        formatter.format(contents)
        block = contents.append(fileName)
        pins = 0
    }
}
