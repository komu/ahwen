package dev.komu.ahwen.metadata

import dev.komu.ahwen.metadata.TableManager.Companion.MAX_NAME
import dev.komu.ahwen.query.StringConstant
import dev.komu.ahwen.record.*
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.IndexName
import dev.komu.ahwen.types.TableName

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

    fun createIndex(indexName: IndexName, tableName: TableName, fieldName: ColumnName, tx: Transaction) {
        TableManager.checkNameLength(indexName.value, COL_INDEX_NAME)
        TableManager.checkNameLength(tableName.value, COL_TABLE_NAME)
        TableManager.checkNameLength(fieldName.value, COL_FIELD_NAME)

        ti.open(tx).use { rf ->
            rf.insertRow(
                COL_INDEX_NAME to StringConstant(indexName.value),
                COL_TABLE_NAME to StringConstant(tableName.value),
                COL_FIELD_NAME to StringConstant(fieldName.value)
            )
        }
    }

    fun getIndexInfo(tableName: TableName, tx: Transaction): Map<ColumnName, IndexInfo> {
        ti.open(tx).use { rf ->
            val result = mutableMapOf<ColumnName, IndexInfo>()
            rf.forEach {
                if (rf.getString(COL_TABLE_NAME) == tableName.value) {
                    val indexName = IndexName(rf.getString(COL_INDEX_NAME))
                    val fieldName = ColumnName(rf.getString(COL_FIELD_NAME))
                    result[fieldName] = IndexInfo(indexName, tableName, fieldName, tx, metadataManager)
                }
            }
            return result
        }
    }

    companion object {
        private val TBL_INDEX_CAT = TableName("idxcat")
        private val COL_INDEX_NAME = ColumnName("indexName")
        private val COL_TABLE_NAME = ColumnName("tableName")
        private val COL_FIELD_NAME = ColumnName("fieldName")
    }
}
