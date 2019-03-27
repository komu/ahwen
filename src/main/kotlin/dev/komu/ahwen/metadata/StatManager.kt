package dev.komu.ahwen.metadata

import dev.komu.ahwen.record.RecordFile
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction

class StatManager(private val tableManager: TableManager, tx: Transaction) {

    private val tableStats = mutableMapOf<String, StatInfo>()
    private var numCalls = 0

    init {
        refreshStatistics(tx)
    }

    @Synchronized
    fun getStatInfo(tableName: String, ti: TableInfo, tx: Transaction): StatInfo {
        numCalls++

        if (numCalls > 100)
            refreshStatistics(tx)

        return tableStats.getOrPut(tableName) {
            calcTableStats(ti, tx)
        }
    }

    @Synchronized
    private fun refreshStatistics(tx: Transaction) {
        tableStats.clear()
        numCalls = 0

        val tcatInfo = tableManager.getTableInfo("tblcat", tx)
        val tcatFile = RecordFile(tcatInfo, tx)
        while (tcatFile.next()) {
            val tableName = tcatFile.getString("tblname")
            val ti = tableManager.getTableInfo(tableName, tx)
            tableStats[tableName] = calcTableStats(ti, tx)
        }
        tcatFile.close()
    }

    @Synchronized
    private fun calcTableStats(ti: TableInfo, tx: Transaction): StatInfo {
        var numRecords = 0
        var numBlocks = 0
        val rf = RecordFile(ti, tx)
        while (rf.next()) {
            numRecords++
            numBlocks = rf.currentRid.blockNumber + 1
        }
        rf.close()
        return StatInfo(numBlocks, numRecords)
    }
}
