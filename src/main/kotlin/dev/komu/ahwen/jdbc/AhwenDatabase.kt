package dev.komu.ahwen.jdbc

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.file.FileManagerImpl
import dev.komu.ahwen.log.LogManager
import dev.komu.ahwen.metadata.MetadataManager
import dev.komu.ahwen.planner.Planner
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.tx.concurrency.LockTable
import java.io.File

class AhwenDatabase(dir: File) {
    private val fileManager = FileManagerImpl(dir)
    private val logManager = LogManager(fileManager, "log")
    private val bufferManager = BufferManager(1000, fileManager, logManager)
    private val lockTable = LockTable()
    private val metadataManager: MetadataManager
    val planner: Planner

    init {
        Transaction(logManager, bufferManager, lockTable, fileManager).also { tx ->
            metadataManager = MetadataManager(fileManager.isNew, tx)
            tx.commit()
        }
        planner = Planner(metadataManager, bufferManager)
    }

    fun beginTransaction() =
        Transaction(logManager, bufferManager, lockTable, fileManager)
}
