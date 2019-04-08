package dev.komu.ahwen.metadata

import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction

/**
 * A facade to database metadata.
 */
class MetadataManager(isNew: Boolean, tx: Transaction) {

    private val tableManager = TableManager(isNew, tx)
    private val viewManager = ViewManager(isNew, tableManager, tx)
    private val indexManager = IndexManager(isNew, tableManager, this, tx)
    private val statManager = StatManager(tableManager, tx)

    fun createTable(name: String, schema: Schema, tx: Transaction) {
        tableManager.createTable(name, schema, tx)
    }

    fun getTableInfo(name: String, tx: Transaction) =
        tableManager.getTableInfo(name, tx)

    fun createView(name: String, def: String, tx: Transaction) {
        viewManager.createView(name, def, tx)
    }

    fun getViewDef(name: String, tx: Transaction): String? =
        viewManager.getViewDef(name, tx)

    fun createIndex(indexName: String, tableName: String, columnName: String, tx: Transaction) {
        indexManager.createIndex(indexName, tableName, columnName, tx)
    }

    fun getIndexInfo(tableName: String, tx: Transaction) =
        indexManager.getIndexInfo(tableName, tx)

    fun getStatInfo(tableName: String, ti: TableInfo, tx: Transaction): StatInfo =
        statManager.getStatInfo(tableName, ti, tx)
}
