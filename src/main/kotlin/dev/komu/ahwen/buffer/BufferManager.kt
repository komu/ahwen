package dev.komu.ahwen.buffer

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.FileManager
import dev.komu.ahwen.log.LogManager
import dev.komu.ahwen.tx.TxNum
import dev.komu.ahwen.utils.await
import java.time.Duration
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Service for managing pinning buffers to blocks. Implements blocking waiting on
 * top of [BasicBufferManager].
 */
class BufferManager(bufferCount: Int, fileManager: FileManager, logManager: LogManager) {

    private val bufferManager = BasicBufferManager(bufferCount, fileManager, logManager)
    private val lock = ReentrantLock()
    private val buffersAvailableCondition = lock.newCondition()

    /**
     * Return a buffer pinned to given [Block].
     *
     * @throws BufferAbortException if no buffers could be freed within [MAX_TIME]
     */
    fun pin(block: Block): Buffer {
        lock.withLock {
            try {
                val startTime = Instant.now()
                var buffer = bufferManager.pin(block)
                while (buffer == null && !waitingTooLong(startTime)) {
                    buffersAvailableCondition.await(MAX_TIME)
                    buffer = bufferManager.pin(block)
                }

                return buffer ?: throw BufferAbortException()

            } catch (e: InterruptedException) {
                throw BufferAbortException()
            }
        }
    }

    /**
     * Allocates a new page from [fileName], formats it with [formatter] and
     * returns a buffer pinned to that page.
     *
     * @throws BufferAbortException if no buffers could be freed within [MAX_TIME]
     */
    fun pinNew(fileName: String, formatter: PageFormatter): Buffer {
        lock.withLock {
            try {
                val startTime = Instant.now()
                var buffer = bufferManager.pinNew(fileName, formatter)
                while (buffer == null && !waitingTooLong(startTime)) {
                    buffersAvailableCondition.await(MAX_TIME)
                    buffer = bufferManager.pinNew(fileName, formatter)
                }

                return buffer ?: throw BufferAbortException()

            } catch (e: InterruptedException) {
                throw BufferAbortException()
            }
        }
    }

    /**
     * Releases a pin on a [Buffer].
     */
    fun unpin(buffer: Buffer) {
        lock.withLock {
            bufferManager.unpin(buffer)
            if (!buffer.isPinned)
                buffersAvailableCondition.signalAll()
        }
    }

    /**
     * Flushes all buffers modified in transaction [txnum].
     */
    fun flushAll(txnum: TxNum) {
        bufferManager.flushAll(txnum)
    }

    /**
     * Returns the number of buffers that are currently unpinned.
     */
    val available: Int
        get() = bufferManager.available

    companion object {

        private val MAX_TIME = Duration.ofSeconds(10)

        private fun waitingTooLong(start: Instant) =
            Duration.between(start, Instant.now()) > MAX_TIME
    }
}
