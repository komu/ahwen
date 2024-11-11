package dev.komu.ahwen.tx.concurrency

import dev.komu.ahwen.file.Block
import dev.komu.ahwen.utils.await
import java.time.Duration
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Stores the locks acquired on blocks.
 */
class LockTable {

    /**
     * Stores locks acquired for the blocks.
     *
     * A missing value means that block has no locks, a value of `-1` means that block has an exclusive lock and
     * a positive value represents the amount of shared locks on the block.
     */
    private val locks = mutableMapOf<Block, Int>()

    private val lock = ReentrantLock()
    private val condition = lock.newCondition()

    /**
     * Acquires a shared (read) lock to given block.
     *
     * @throws LockAbortException if timeout occurs
     */
    fun sLock(block: Block) {
        lock.withLock {
            try {
                val startTime = Instant.now()
                while (hasXlock(block) && !waitingTooLong(startTime)) {
                    condition.await(MAX_TIME)
                }

                if (hasXlock(block))
                    throw LockAbortException()

                val value = getLockVal(block)
                locks[block] = value + 1

            } catch (_: InterruptedException) {
                throw LockAbortException()
            }
        }
    }

    /**
     * Acquires an exclusive (write) lock to given block.
     *
     * @throws LockAbortException if timeout or deadlock occurs
     */
    fun xLock(block: Block) {
        lock.withLock {
            try {
                val startTime = Instant.now()
                while (hasOtherSLocks(block) && !waitingTooLong(startTime)) {
                    condition.await(MAX_TIME)
                }

                if (hasOtherSLocks(block))
                    throw LockAbortException()

                locks[block] = -1

            } catch (_: InterruptedException) {
                throw LockAbortException()
            }
        }
    }

    /**
     * Releases a lock on the block.
     */
    fun unlock(block: Block) {
        lock.withLock {
            val value = getLockVal(block)
            if (value > 1)
                locks[block] = value - 1
            else
                locks.remove(block)
            condition.signalAll()
        }
    }

    private fun hasXlock(block: Block): Boolean =
        getLockVal(block) < 0

    private fun hasOtherSLocks(block: Block): Boolean =
        getLockVal(block) > 1

    private fun getLockVal(block: Block): Int =
        locks[block] ?: 0

    companion object {

        private fun waitingTooLong(startTime: Instant): Boolean =
            Duration.between(startTime, Instant.now()) > MAX_TIME

        private val MAX_TIME = Duration.ofSeconds(10)
    }
}
