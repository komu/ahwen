package dev.komu.ahwen.metadata

import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.record.forEach
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.SqlType

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

    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        checkNameLength(tableName, "name")
        val ti = TableInfo(tableName, schema)

        // insert one record into tblcat
        tableCatalogInfo.open(tx).use { tcatFile ->
            tcatFile.insert()
            tcatFile.setString(COL_TABLE_NAME, tableName)
            tcatFile.setInt(COL_REC_LENGTH, ti.recordLength)
        }

        // insert a record into fldcat for each field
        fieldCatalogInfo.open(tx).use { fcat ->
            for (field in schema.fields) {
                fcat.insert()
                fcat.setString(COL_TABLE_NAME, tableName)
                fcat.setString(COL_FIELD_NAME, field)
                fcat.setInt(COL_TYPE, schema.type(field).code)
                fcat.setInt(COL_LENGTH, schema.length(field))
                fcat.setInt(COL_OFFSET, ti.offset(field))
            }
        }
    }

    fun getTableInfo(tableName: String, tx: Transaction): TableInfo {
        val recLen = getRecordLength(tx, tableName)

        val schema = Schema.Builder()
        val offsets = mutableMapOf<String, Int>()

        fieldCatalogInfo.open(tx).use { fcat ->
            fcat.forEach {
                if (fcat.getString(COL_TABLE_NAME) == tableName) {
                    val field = fcat.getString(COL_FIELD_NAME)
                    val type = SqlType(fcat.getInt(COL_TYPE))
                    val length = fcat.getInt(COL_LENGTH)

                    offsets[field] = fcat.getInt(COL_OFFSET)
                    schema.addField(field, type, length)
                }
            }
        }

        return TableInfo(tableName, schema.build(), offsets, recLen)
    }

    private fun getRecordLength(tx: Transaction, tableName: String): Int {
        tableCatalogInfo.open(tx).use { tcat ->
            tcat.forEach {
                if (tcat.getString(COL_TABLE_NAME) == tableName)
                    return tcat.getInt(COL_REC_LENGTH)
            }
        }

        error("could not find table $tableName")
    }

    companion object {
        const val MAX_NAME = 16

        private const val TBL_TABLE_CAT = "tblcat"
        private const val TBL_FIELD_CAT = "fldcat"
        private const val COL_TABLE_NAME = "tblname"
        private const val COL_FIELD_NAME = "fldname"
        private const val COL_TYPE = "type"
        private const val COL_LENGTH = "length"
        private const val COL_OFFSET = "offset"
        private const val COL_REC_LENGTH = "reclength"

        fun checkNameLength(name: String, description: String) {
            require(name.length <= MAX_NAME) { "max name length is $MAX_NAME, but $description '$name' is '${name.length}"}
        }
    }
}
