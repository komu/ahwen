package dev.komu.ahwen.buffer

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.FileManager
import dev.komu.ahwen.file.Page
import dev.komu.ahwen.log.LSN
import dev.komu.ahwen.log.LogManager
import dev.komu.ahwen.tx.TxNum
import dev.komu.ahwen.types.FileName

/**
 * Buffer is reusable [Page] of data that can be _pinned_ to a certain [Block].
 * Buffers are the primary mechanism of caching: [BasicBufferManager] maintains
 * a pool of buffers that the database can then allocate to reduce reads from
 * disk.
 *
 * Buffer may reside in one of three states:
 *
 * 1. It might not be assigned to any block. In this case the buffer has no meaningful
 *    data and is a good candidate to use for anything that needs a buffer.
 * 2. It is _pinned_ to a block. Pinning means that some transaction is actively using
 *    the buffer and we may not use the buffer for other purposes until it is unpinned.
 *    However, multiple transactions may use the buffer to the same block, increasing
 *    the count of pins on the buffer.
 * 3. It might be assigned to a block without being pinned. This means that we are free
 *    to use the buffer for any purpose (i.e. we can assign it to some other block if
 *    needed), but if we need the data for the already assigned block, we're in luck,
 *    since we don't need to reload it from the disk; it suffices to pin the buffer.
 *
 * Finally, if the buffer is modified by a transaction, we store the id of the transaction
 * along with the [LSN] of log so that we can flush the buffer when the transaction commits.
 */
class Buffer(fileManager: FileManager, private val logManager: LogManager) {

    private val contents = Page(fileManager)
    var block: Block? = null
        private set
    private var pins = 0
    private var modifiedBy: TxNum? = null
    private var logSequenceNumber = LSN.zero

    fun getInt(offset: Int): Int =
        contents.getInt(offset)

    fun getString(offset: Int): String =
        contents.getString(offset)

    fun setInt(offset: Int, value: Int, txnum: TxNum, lsn: LSN) {
        modifiedBy = txnum
        if (lsn >= LSN.zero)
            logSequenceNumber = lsn
        contents.setInt(offset, value)
    }

    fun setString(offset: Int, value: String, txnum: TxNum, lsn: LSN) {
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

    fun isModifiedBy(txnum: TxNum) =
        txnum == modifiedBy

    fun assignToBlock(block: Block) {
        flush()
        this.block = block
        contents.read(block)
        pins = 0
    }

    fun assignToNew(fileName: FileName, formatter: PageFormatter) {
        flush()
        formatter.format(contents)
        block = contents.append(fileName)
        pins = 0
    }
}
