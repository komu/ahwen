package dev.komu.ahwen.query.materialize

import dev.komu.ahwen.query.TableScan
import dev.komu.ahwen.query.UpdateScan
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.TableName
import java.util.concurrent.atomic.AtomicInteger

/**
 * Represents a temporary table. Each instantiated table is distinct from all other tables.
 *
 * TODO: Currently the temporary tables are never released. Temporary tables will be deleted
 *       during system startup, but as long as the server is running, they will be kept.
 *       Relatively straightforward way to fix this would be to associate them with transactions
 *       and clear them when transaction finishes. However, for long running transactions this
 *       could cause the tables to be kept around longer than necessary.
 *
 * TODO: Currently temporary tables are just tables named `tempXXX` where XXX is a running number.
 *       Nothing prevents normal queries from accessing these same tables.
 */
class TempTable(schema: Schema, private val tx: Transaction) {

    val tableInfo = TableInfo(nextTableName(), schema)

    fun open(): UpdateScan =
        TableScan(tableInfo, tx)

    companion object {

        private val nextTableNum = AtomicInteger(0)

        private fun nextTableName() =
            TableName.temporary(nextTableNum.incrementAndGet())
    }
}
