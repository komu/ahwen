package dev.komu.ahwen.metadata

import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.record.forEach
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.SqlType
import dev.komu.ahwen.types.TableName

/**
 * Class responsible for maintaining tables.
 */
class TableManager(isNew: Boolean, tx: Transaction) {

    private val tableCatalogInfo = TableInfo(TBL_TABLE_CAT, Schema {
        stringField(COL_TABLE_NAME, MAX_NAME)
        intField(COL_REC_LENGTH)
    })

    private val fieldCatalogInfo = TableInfo(TBL_FIELD_CAT, Schema {
        stringField(COL_TABLE_NAME, MAX_NAME)
        stringField(COL_FIELD_NAME, MAX_NAME)
        intField(COL_TYPE)
        intField(COL_LENGTH)
        intField(COL_OFFSET)
    })

    init {
        if (isNew) {
            createTable(tableCatalogInfo.tableName, tableCatalogInfo.schema, tx)
            createTable(fieldCatalogInfo.tableName, fieldCatalogInfo.schema, tx)
        }
    }

    fun createTable(tableName: TableName, schema: Schema, tx: Transaction) {
        checkNameLength(tableName.value, COL_TABLE_NAME)
        val ti = TableInfo(tableName, schema)

        // insert one record into tblcat
        tableCatalogInfo.open(tx).use { tcatFile ->
            tcatFile.insert()
            tcatFile.setString(COL_TABLE_NAME, tableName.value)
            tcatFile.setInt(COL_REC_LENGTH, ti.recordLength)
        }

        // insert a record into fldcat for each field
        fieldCatalogInfo.open(tx).use { fcat ->
            for (column in schema.columns) {
                val info = schema[column]
                fcat.insert()
                fcat.setString(COL_TABLE_NAME, tableName.value)
                fcat.setString(COL_FIELD_NAME, column.value)
                fcat.setInt(COL_TYPE, info.type.code)
                fcat.setInt(COL_LENGTH, info.length)
                fcat.setInt(COL_OFFSET, ti.offset(column))
            }
        }
    }

    fun getTableInfo(tableName: TableName, tx: Transaction): TableInfo {
        val recLen = getRecordLength(tx, tableName)

        val schema = Schema.Builder()
        val offsets = mutableMapOf<ColumnName, Int>()

        fieldCatalogInfo.open(tx).use { fcat ->
            fcat.forEach {
                if (fcat.getString(COL_TABLE_NAME) == tableName.value) {
                    val field = ColumnName(fcat.getString(COL_FIELD_NAME))
                    val type = SqlType(fcat.getInt(COL_TYPE))
                    val length = fcat.getInt(COL_LENGTH)

                    offsets[field] = fcat.getInt(COL_OFFSET)
                    schema.addField(field, type, length)
                }
            }
        }

        return TableInfo(tableName, schema.build(), offsets, recLen)
    }

    private fun getRecordLength(tx: Transaction, tableName: TableName): Int {
        tableCatalogInfo.open(tx).use { tcat ->
            tcat.forEach {
                if (tcat.getString(COL_TABLE_NAME) == tableName.value)
                    return tcat.getInt(COL_REC_LENGTH)
            }
        }

        error("could not find table $tableName")
    }

    companion object {
        const val MAX_NAME = 16

        val TBL_TABLE_CAT = TableName("tblcat")
        private val TBL_FIELD_CAT = TableName("fldcat")
        val COL_TABLE_NAME = ColumnName("tblname")
        private val COL_FIELD_NAME = ColumnName("fldname")
        private val COL_TYPE = ColumnName("type")
        private val COL_LENGTH = ColumnName("length")
        private val COL_OFFSET = ColumnName("offset")
        private val COL_REC_LENGTH = ColumnName("reclength")

        fun checkNameLength(name: String, column: ColumnName) {
            require(name.length <= MAX_NAME) { "max name length is $MAX_NAME, but $column '$name' is '${name.length}"}
        }
    }
}
