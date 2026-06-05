package dev.komu.ahwen.buffer

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.FileManager
import dev.komu.ahwen.log.LogManager
import dev.komu.ahwen.tx.TxNum
import dev.komu.ahwen.types.FileName
import dev.komu.ahwen.utils.LRUSet
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

    /** All buffers in the system */
    private val bufferPool = List(bufferCount) { Buffer(fileManager, logManager) }

    /** Buffers sorted by their usage */
    private val lru = LRUSet(bufferPool)

    /** A cache from blocks to their corresponding buffers. */
    private val buffersByBlocks = mutableMapOf<Block, Buffer>()

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

                removeOldBlock(buffer)
                buffer.assignToBlock(block)
                buffersByBlocks[block] = buffer
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

            removeOldBlock(buffer)
            val newBlock = buffer.assignToNew(fileName, formatter)
            buffersByBlocks[newBlock] = buffer
            available--
            buffer.pin()
            return buffer
        }
    }

    fun unpin(buffer: Buffer) {
        lock.withLock {
            buffer.unpin()
            if (!buffer.isPinned) {
                available++
                lru.touch(buffer)
            }
        }
    }

    private fun removeOldBlock(buffer: Buffer) {
        val block = buffer.block
        if (block != null)
            buffersByBlocks.remove(block)
    }

    private fun findExistingBuffer(block: Block): Buffer? =
        buffersByBlocks[block]?.also {
            assert(it.block == block) { "${it.block} != $block" }
        }

    /**
     * Returns an unpinned buffer to assign to another block.
     */
    private fun chooseUnpinnedBuffer(): Buffer? =
        lru.find { !it.isPinned }
}
