package dev.komu.ahwen.metadata

import dev.komu.ahwen.metadata.TableManager.Companion.MAX_NAME
import dev.komu.ahwen.record.RecordFile
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction

class IndexManager(
    isNew: Boolean,
    tableManager: TableManager,
    private val metadataManager: MetadataManager,
    tx: Transaction
) {
    private val ti: TableInfo

    init {
        if (isNew) {
            tableManager.createTable("idxcat", Schema().apply {
                addStringField("indexname", MAX_NAME)
                addStringField("tablename", MAX_NAME)
                addStringField("fieldname", MAX_NAME)
            }, tx)
        }

        ti = tableManager.getTableInfo("idxcat", tx)
    }

    fun createIndex(indexName: String, tableName: String, fieldName: String, tx: Transaction) {
        TableManager.checkNameLength(indexName, "indexName")
        TableManager.checkNameLength(tableName, "tableName")
        TableManager.checkNameLength(fieldName, "fieldName")

        val rf = RecordFile(ti, tx)
        rf.insert()
        rf.setString("indexname", indexName)
        rf.setString("tablename", tableName)
        rf.setString("fieldname", fieldName)
        rf.close()
    }

    fun getIndexInfo(tableName: String, tx: Transaction): Map<String, IndexInfo> {
        val rf = RecordFile(ti, tx)
        val result = mutableMapOf<String, IndexInfo>()
        while (rf.next()) {
            if (rf.getString("tablename") == tableName) {
                val indexName = rf.getString("indexname")
                val fieldName = rf.getString("fieldname")
                result[fieldName] = IndexInfo(indexName, tableName, fieldName, tx, metadataManager)
            }
        }
        rf.close()
        return result
    }
}
