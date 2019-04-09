package dev.komu.ahwen.buffer

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.FileManager
import dev.komu.ahwen.log.LogManager
import dev.komu.ahwen.tx.TxNum
import dev.komu.ahwen.types.FileName
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Low level service for managing buffers.
 *
 * This class provides non-blocking methods for pinning buffers to blocks. If buffers are not
 * available, the methods simply return `null` to indicate failure. Typical code uses [BufferManager],
 * which implement waiting functionality on top of this class.
 */
class BasicBufferManager(bufferCount: Int, fileManager: FileManager, logManager: LogManager) {

    private val bufferPool = List(bufferCount) { Buffer(fileManager, logManager) }

    /** The amount of available buffers in the pool */
    @Volatile
    var available = bufferPool.size
        private set

    private val lock = ReentrantLock()

    fun flushAll(txnum: TxNum) {
        lock.withLock {
            // TODO: having to walk through all buffers to commit tx is not nice
            for (buffer in bufferPool)
                if (buffer.isModifiedBy(txnum))
                    buffer.flush()
        }
    }

    fun pin(block: Block): Buffer? {
        lock.withLock {
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
    }

    fun pinNew(fileName: FileName, formatter: PageFormatter): Buffer? {
        lock.withLock {
            val buffer = chooseUnpinnedBuffer()
                ?: return null

            buffer.assignToNew(fileName, formatter)
            available--
            buffer.pin()
            return buffer
        }
    }

    fun unpin(buffer: Buffer) {
        lock.withLock {
            buffer.unpin()
            if (!buffer.isPinned)
                available++
        }
    }

    // TODO: maintain a map from blocks to buffers so that we don't have to do an O(n) walk
    private fun findExistingBuffer(block: Block): Buffer? =
        bufferPool.find { it.block == block }

    /**
     * Returns an unpinned buffer to assign to another block.
     *
     * TODO: Returning the first unpinned buffer is as simple as it gets, but is far from optimal.
     */
    private fun chooseUnpinnedBuffer(): Buffer? =
        bufferPool.find { !it.isPinned }
}
