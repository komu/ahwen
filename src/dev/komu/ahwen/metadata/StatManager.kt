package dev.komu.ahwen.metadata

import dev.komu.ahwen.metadata.TableManager.Companion.COL_TABLE_NAME
import dev.komu.ahwen.metadata.TableManager.Companion.TBL_TABLE_CAT
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.record.forEach
import dev.komu.ahwen.record.getString
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.TableName
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Maintains statistics about tables.
 */
class StatManager(private val tableManager: TableManager, tx: Transaction) {

    private val tableStats = mutableMapOf<TableName, StatInfo>()
    private var numCalls = 0
    private val lock = ReentrantLock()

    init {
        refreshStatistics(tx)
    }

    fun getStatInfo(tableName: TableName, ti: TableInfo, tx: Transaction): StatInfo {
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

            val tcatInfo = tableManager.getTableInfo(TBL_TABLE_CAT, tx)
            tcatInfo.open(tx).use { tcatFile ->
                tcatFile.forEach {
                    val tableName = TableName(tcatFile.getString(COL_TABLE_NAME))
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
