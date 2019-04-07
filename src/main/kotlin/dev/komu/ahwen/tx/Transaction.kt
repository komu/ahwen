package dev.komu.ahwen.tx

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.buffer.PageFormatter
import dev.komu.ahwen.file.Block
import dev.komu.ahwen.file.FileManager
import dev.komu.ahwen.log.LogManager
import dev.komu.ahwen.tx.concurrency.ConcurrencyManager
import dev.komu.ahwen.tx.concurrency.LockTable
import dev.komu.ahwen.tx.recovery.RecoveryManager
import java.util.concurrent.atomic.AtomicInteger

/**
 * A facade for managing transactional operations. Integrates with [RecoveryManager]
 * and [ConcurrencyManager] to provide isolation and durability guarantees for data.
 *
 * Currently implements only serializable transaction level. This is because all locks
 * are held to the end of transaction and the exclusive lock taken on [append] will
 * prevent [size] from taking the file size if new blocks are appended.
 *
 * Less strict isolation levels could be achieved by:
 *
 * - repeatable read: don't acquire lock on [size]
 * - read committed: don't acquire lock on [size], release shared locks before commit
 * - read uncommitted: never acquire shared locks
 */
class Transaction(logManager: LogManager, bufferManager: BufferManager, lockTable: LockTable, private val fileManager: FileManager) {

    private val txnum = TxNum(nextTxNum.getAndDecrement())
    private val recoveryManager = RecoveryManager(txnum, logManager, bufferManager)
    private val concurrencyManager = ConcurrencyManager(lockTable)
    private val myBuffers = BufferList(bufferManager)

    fun commit() {
        myBuffers.unpinAll()
        recoveryManager.commit()
        concurrencyManager.release()
    }

    fun rollback() {
        myBuffers.unpinAll()
        recoveryManager.rollback()
        concurrencyManager.release()
    }

    fun recover() {
        recoveryManager.recover()
    }

    fun pin(block: Block) {
        myBuffers.pin(block)
    }

    fun unpin(block: Block) {
        myBuffers.unpin(block)
    }

    fun getInt(block: Block, offset: Int): Int {
        concurrencyManager.sLock(block)
        val buffer = myBuffers.getBuffer(block)
        return buffer.getInt(offset)
    }

    fun getString(block: Block, offset: Int): String {
        concurrencyManager.sLock(block)
        val buffer = myBuffers.getBuffer(block)
        return buffer.getString(offset)
    }

    fun setInt(block: Block, offset: Int, value: Int) {
        concurrencyManager.xLock(block)
        val buffer = myBuffers.getBuffer(block)
        val lsn = recoveryManager.setInt(buffer, offset, value)
        buffer.setInt(offset, value, txnum, lsn)
    }

    fun setString(block: Block, offset: Int, value: String) {
        concurrencyManager.xLock(block)
        val buffer = myBuffers.getBuffer(block)
        val lsn = recoveryManager.setString(buffer, offset, value)
        buffer.setString(offset, value, txnum, lsn)
    }

    fun size(fileName: String): Int {
        concurrencyManager.sLock(eofBlock(fileName))
        return fileManager.size(fileName)
    }

    fun append(fileName: String, formatter: PageFormatter): Block {
        concurrencyManager.xLock(eofBlock(fileName))
        val block = myBuffers.pinNew(fileName, formatter)
        unpin(block)
        return block
    }

    companion object {
        private val nextTxNum = AtomicInteger(0)

        /**
         * Returns a dummy block representing the end-of-file for given file. The block can't be
         * read since it has an invalid number, but it can be locked to achieve serializable isolation.
         */
        private fun eofBlock(fileName: String) = Block(fileName, -1)
    }
}
