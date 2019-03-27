package dev.komu.ahwen.buffer

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.FileManager
import dev.komu.ahwen.log.LogManager

class BasicBufferManager(bufferCount: Int, fileManager: FileManager, logManager: LogManager) {

    private val bufferPool = List(bufferCount) { Buffer(fileManager, logManager) }
    var available = bufferPool.size
        private set

    @Synchronized
    fun flushAll(txnum: Int) {
        for (buffer in bufferPool)
            if (buffer.isModifiedBy(txnum))
                buffer.flush()
    }

    @Synchronized
    fun pin(block: Block): Buffer? {
        var buffer = findExistingBuffer(block)
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer()
                ?: return null
            buffer.assignToBlock(block)
        }

        if (!buffer.isPinned)
            available--
        buffer.pin()

        return buffer
    }

    @Synchronized
    fun pinNew(fileName: String, formatter: PageFormatter): Buffer? {
        val buffer = chooseUnpinnedBuffer()
            ?: return null

        buffer.assignToNew(fileName, formatter)
        available--
        buffer.pin()
        return buffer
    }

    @Synchronized
    fun unpin(buffer: Buffer) {
        buffer.unpin()
        if (!buffer.isPinned)
            available++
    }

    private fun findExistingBuffer(block: Block): Buffer? =
        bufferPool.find { it.block == block }

    private fun chooseUnpinnedBuffer(): Buffer? =
        bufferPool.find { !it.isPinned }
}
