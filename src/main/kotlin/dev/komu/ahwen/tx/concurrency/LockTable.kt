package dev.komu.ahwen.tx.concurrency

import dev.komu.ahwen.file.Block
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class LockTable {

    private val lock = ReentrantLock()
    private val condition = lock.newCondition()
    private val locks = mutableMapOf<Block, Int>()

    fun sLock(block: Block) {
        lock.withLock {
            try {
                val startTime = Instant.now()
                while (hasXlock(block) && !waitingTooLong(startTime)) {
                    condition.await(MAX_TIME.toMillis(), TimeUnit.MILLISECONDS)
                }

                if (hasXlock(block))
                    throw LockAbortException()

                val value = getLockVal(block)
                locks[block] = value + 1

            } catch (e: InterruptedException) {
                throw LockAbortException()
            }
        }
    }

    fun xLock(block: Block) {
        lock.withLock {
            try {
                val startTime = Instant.now()
                while (hasOtherSLocks(block) && !waitingTooLong(startTime)) {
                    condition.await(MAX_TIME.toMillis(), TimeUnit.MILLISECONDS)
                }

                if (hasOtherSLocks(block))
                    throw LockAbortException()

                locks[block] = -1

            } catch (e: InterruptedException) {
                throw LockAbortException()
            }
        }
    }

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
