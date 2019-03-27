@file:Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")

package dev.komu.ahwen.buffer

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.FileManager
import dev.komu.ahwen.log.LogManager
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class BufferManager(bufferCount: Int, fileManager: FileManager, logManager: LogManager) {

    private val lock = ReentrantLock()
    private val buffersAvailableCondition = lock.newCondition()

    private val bufferManager = BasicBufferManager(bufferCount, fileManager, logManager)

    fun pin(block: Block): Buffer {
        lock.withLock {
            try {
                val startTime = Instant.now()
                var buffer = bufferManager.pin(block)
                while (buffer == null && !waitingTooLong(startTime)) {
                    buffersAvailableCondition.await(MAX_TIME.toMillis(), TimeUnit.MILLISECONDS)
                    buffer = bufferManager.pin(block)
                }

                return buffer ?: throw BufferAbortException()

            } catch (e: InterruptedException) {
                throw BufferAbortException()
            }
        }
    }

    fun pinNew(fileName: String, formatter: PageFormatter): Buffer {
        lock.withLock {
            try {
                val startTime = Instant.now()
                var buffer = bufferManager.pinNew(fileName, formatter)
                while (buffer == null && !waitingTooLong(startTime)) {
                    buffersAvailableCondition.await(MAX_TIME.toMillis(), TimeUnit.MILLISECONDS)
                    buffer = bufferManager.pinNew(fileName, formatter)
                }

                return buffer ?: throw BufferAbortException()

            } catch (e: InterruptedException) {
                throw BufferAbortException()
            }
        }
    }

    fun unpin(buffer: Buffer) {
        lock.withLock {
            bufferManager.unpin(buffer)
            if (!buffer.isPinned)
                buffersAvailableCondition.signalAll()
        }
    }

    fun flushAll(txnum: Int) {
        bufferManager.flushAll(txnum)
    }

    val available: Int
        get() = bufferManager.available

    private fun waitingTooLong(start: Instant) =
        Duration.between(start, Instant.now()) > MAX_TIME

    companion object {

        private val MAX_TIME = Duration.ofSeconds(10)
    }
}
