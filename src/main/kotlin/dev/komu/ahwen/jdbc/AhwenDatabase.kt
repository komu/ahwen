package dev.komu.ahwen.jdbc

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.file.DefaultFileManager
import dev.komu.ahwen.file.FileStats
import dev.komu.ahwen.log.LogManager
import dev.komu.ahwen.metadata.MetadataManager
import dev.komu.ahwen.planner.Planner
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.tx.concurrency.LockTable
import dev.komu.ahwen.types.FileName
import java.io.File

class AhwenDatabase(dir: File) {
    private val fileManager = DefaultFileManager(dir)
    private val logManager = LogManager(fileManager, FileName("log"))
    private val bufferManager = BufferManager(1000, fileManager, logManager)
    private val lockTable = LockTable()
    private val metadataManager: MetadataManager
    val planner: Planner

    val fileStats: FileStats
        get() = fileManager.stats

    init {
        Transaction(logManager, bufferManager, lockTable, fileManager).also { tx ->
            tx.recover()

            metadataManager = MetadataManager(fileManager.isNew, tx)
            tx.commit()
        }
        planner = Planner(metadataManager, bufferManager)
    }

    fun beginTransaction() =
        Transaction(logManager, bufferManager, lockTable, fileManager)
}
