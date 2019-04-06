package dev.komu.ahwen.tx.concurrency

import dev.komu.ahwen.file.Block

/**
 * Manages the locks for a single transaction.
 *
 * Keeps track of the locks already acquired for this transaction so that:
 *
 * 1. we don't try to acquire the same lock multiple times
 * 2. we can release all the locks when the transaction ends
 */
class ConcurrencyManager(private val lockTable: LockTable) {

    private val locks = mutableMapOf<Block, LockType>()

    fun sLock(block: Block) {
        if (block !in locks) {
            lockTable.sLock(block)
            locks[block] = LockType.SHARED
        }
    }

    fun xLock(block: Block) {
        if (locks[block] != LockType.EXCLUSIVE) {
            sLock(block)
            lockTable.xLock(block)
            locks[block] = LockType.EXCLUSIVE
        }
    }

    fun release() {
        for (block in locks.keys)
            lockTable.unlock(block)
        locks.clear()
    }

    private enum class LockType {
        SHARED, EXCLUSIVE
    }
}
