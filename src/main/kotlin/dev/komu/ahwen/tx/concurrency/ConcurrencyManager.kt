package dev.komu.ahwen.tx.concurrency

import dev.komu.ahwen.file.Block

class ConcurrencyManager(private val lockTable: LockTable) {

    private val locks = mutableMapOf<Block, String>()

    fun sLock(block: Block) {
        if (block !in locks) {
            lockTable.sLock(block)
            locks[block] = "S"
        }
    }

    fun xLock(block: Block) {
        if (locks[block] != "X") {
            sLock(block)
            lockTable.xLock(block)
            locks[block] = "X"
        }
    }

    fun release() {
        for (block in locks.keys)
            lockTable.unlock(block)
        locks.clear()
    }
}
