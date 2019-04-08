package dev.komu.ahwen.metadata

import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.record.forEach
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.SqlType

class TableManager(isNew: Boolean, tx: Transaction) {

    private val tcatInfo = TableInfo("tblcat", Schema {
        stringField("tblname", MAX_NAME)
        intField("reclength")
    })
    private val fcatInfo = TableInfo("fldcat", Schema {
        stringField("tblname", MAX_NAME)
        stringField("fldname", MAX_NAME)
        intField("type")
        intField("length")
        intField("offset")
    })

    init {
        if (isNew) {
            createTable(tcatInfo.tableName, tcatInfo.schema, tx)
            createTable(fcatInfo.tableName, fcatInfo.schema, tx)
        }
    }

    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        checkNameLength(tableName, "name")
        val ti = TableInfo(tableName, schema)

        // insert one record into tblcat
        tcatInfo.open(tx).use { tcatFile ->
            tcatFile.insert()
            tcatFile.setString("tblname", tableName)
            tcatFile.setInt("reclength", ti.recordLength)
        }

        // insert a record into fldcat for each field
        fcatInfo.open(tx).use { fcatFile ->
            for (field in schema.fields) {
                fcatFile.insert()
                fcatFile.setString("tblname", tableName)
                fcatFile.setString("fldname", field)
                fcatFile.setInt("type", schema.type(field).code)
                fcatFile.setInt("length", schema.length(field))
                fcatFile.setInt("offset", ti.offset(field))
            }
        }
    }

    fun getTableInfo(tableName: String, tx: Transaction): TableInfo {
        var recLen = -1
        tcatInfo.open(tx).use { tcatFile ->
            while (tcatFile.next()) {
                if (tcatFile.getString("tblname") == tableName) {
                    recLen = tcatFile.getInt("reclength")
                    break
                }
            }
        }

        if (recLen == -1) error("could not find table $tableName")

        val schema = Schema.Builder()
        val offsets = mutableMapOf<String, Int>()

        fcatInfo.open(tx).use { fcatFile ->
            fcatFile.forEach {
                if (fcatFile.getString("tblname") == tableName) {
                    val field = fcatFile.getString("fldname")
                    val type = SqlType(fcatFile.getInt("type"))
                    val length = fcatFile.getInt("length")

                    offsets[field] = fcatFile.getInt("offset")
                    schema.addField(field, type, length)
                }
            }
        }

        return TableInfo(tableName, schema.build(), offsets, recLen)
    }

    companion object {
        const val MAX_NAME = 16

        fun checkNameLength(name: String, description: String) {
            require(name.length <= MAX_NAME) { "max name length is $MAX_NAME, but $description '$name' is '${name.length}"}
        }
    }
}
