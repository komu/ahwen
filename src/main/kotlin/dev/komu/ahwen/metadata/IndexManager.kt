package dev.komu.ahwen.metadata

import dev.komu.ahwen.metadata.TableManager.Companion.MAX_NAME
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.record.forEach
import dev.komu.ahwen.tx.Transaction

/**
 * Class responsible for maintaining indices.
 */
class IndexManager(
    isNew: Boolean,
    tableManager: TableManager,
    private val metadataManager: MetadataManager,
    tx: Transaction
) {
    private val ti: TableInfo

    init {
        if (isNew) {
            tableManager.createTable(TBL_INDEX_CAT, Schema {
                stringField(COL_INDEX_NAME, MAX_NAME)
                stringField(COL_TABLE_NAME, MAX_NAME)
                stringField(COL_FIELD_NAME, MAX_NAME)
            }, tx)
        }

        ti = tableManager.getTableInfo(TBL_INDEX_CAT, tx)
    }

    fun createIndex(indexName: String, tableName: String, fieldName: String, tx: Transaction) {
        TableManager.checkNameLength(indexName, COL_INDEX_NAME)
        TableManager.checkNameLength(tableName, COL_TABLE_NAME)
        TableManager.checkNameLength(fieldName, COL_FIELD_NAME)

        ti.open(tx).use { rf ->
            rf.insert()
            rf.setString(COL_INDEX_NAME, indexName)
            rf.setString(COL_TABLE_NAME, tableName)
            rf.setString(COL_FIELD_NAME, fieldName)
        }
    }

    fun getIndexInfo(tableName: String, tx: Transaction): Map<String, IndexInfo> {
        ti.open(tx).use { rf ->
            val result = mutableMapOf<String, IndexInfo>()
            rf.forEach {
                if (rf.getString(COL_TABLE_NAME) == tableName) {
                    val indexName = rf.getString(COL_INDEX_NAME)
                    val fieldName = rf.getString(COL_FIELD_NAME)
                    result[fieldName] = IndexInfo(indexName, tableName, fieldName, tx, metadataManager)
                }
            }
            return result
        }
    }

    companion object {
        private const val TBL_INDEX_CAT = "idxcat"
        private const val COL_INDEX_NAME = "indexName"
        private const val COL_TABLE_NAME = "tableName"
        private const val COL_FIELD_NAME = "fieldName"
    }
}
