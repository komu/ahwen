package dev.komu.ahwen.metadata

import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.record.forEach
import dev.komu.ahwen.tx.Transaction
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Maintains statistics about tables.
 */
class StatManager(private val tableManager: TableManager, tx: Transaction) {

    private val tableStats = mutableMapOf<String, StatInfo>()
    private var numCalls = 0
    private val lock = ReentrantLock()

    init {
        refreshStatistics(tx)
    }

    fun getStatInfo(tableName: String, ti: TableInfo, tx: Transaction): StatInfo {
        lock.withLock {
            numCalls++

            if (numCalls > 100)
                refreshStatistics(tx)

            return tableStats.getOrPut(tableName) {
                calcTableStats(ti, tx)
            }
        }
    }

    private fun refreshStatistics(tx: Transaction) {
        lock.withLock {
            tableStats.clear()
            numCalls = 0

            val tcatInfo = tableManager.getTableInfo("tblcat", tx)
            tcatInfo.open(tx).use { tcatFile ->
                tcatFile.forEach {
                    val tableName = tcatFile.getString("tblname")
                    val ti = tableManager.getTableInfo(tableName, tx)
                    tableStats[tableName] = calcTableStats(ti, tx)
                }
            }
        }
    }

    private fun calcTableStats(ti: TableInfo, tx: Transaction): StatInfo {
        lock.withLock {
            var numRecords = 0
            var numBlocks = 0
            ti.open(tx).use { rf ->
                rf.forEach {
                    numRecords++
                    numBlocks = rf.currentRid.blockNumber + 1
                }
            }
            return StatInfo(numBlocks, numRecords)
        }
    }
}
