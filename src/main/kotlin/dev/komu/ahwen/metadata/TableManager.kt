package dev.komu.ahwen.metadata

import dev.komu.ahwen.record.RecordFile
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction

class TableManager(isNew: Boolean, tx: Transaction) {

    private val tcatInfo = TableInfo("tblcat", Schema().apply {
        addStringField("tblname", MAX_NAME)
        addIntField("reclength")
    })
    private val fcatInfo = TableInfo("fldcat", Schema().apply {
        addStringField("tblname", MAX_NAME)
        addStringField("fldname", MAX_NAME)
        addIntField("type")
        addIntField("length")
        addIntField("offset")
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
        val tcatFile = RecordFile(tcatInfo, tx)
        tcatFile.insert()
        tcatFile.setString("tblname", tableName)
        tcatFile.setInt("reclength", ti.recordLength)
        tcatFile.close()

        // insert a record into fldcat for each field
        val fcatFile = RecordFile(fcatInfo, tx)
        for (field in schema.fields) {
            fcatFile.insert()
            fcatFile.setString("tblname", tableName)
            fcatFile.setString("fldname", field)
            fcatFile.setInt("type", schema.type(field))
            fcatFile.setInt("length", schema.length(field))
            fcatFile.setInt("offset", ti.offset(field))
        }
        fcatFile.close()
    }

    fun getTableInfo(tableName: String, tx: Transaction): TableInfo {
        val tcatFile = RecordFile(tcatInfo, tx)
        var recLen = -1
        while (tcatFile.next()) {
            if (tcatFile.getString("tblname") == tableName) {
                recLen = tcatFile.getInt("reclength")
                break
            }
        }
        tcatFile.close()

        if (recLen == -1) error("could not find table $tableName")

        val schema = Schema()
        val offsets = mutableMapOf<String, Int>()

        val fcatFile = RecordFile(fcatInfo, tx)
        while (fcatFile.next()) {
            if (fcatFile.getString("tblname") == tableName) {
                val field = fcatFile.getString("fldname")
                val type = fcatFile.getInt("type")
                val length = fcatFile.getInt("length")

                offsets[field] = fcatFile.getInt("offset")
                schema.addField(field, type, length)
            }
        }
        fcatFile.close()
        return TableInfo(tableName, schema, offsets, recLen)
    }

    companion object {
        const val MAX_NAME = 16

        fun checkNameLength(name: String, description: String) {
            require(name.length <= MAX_NAME) { "max name length is $MAX_NAME, but $description '$name' is '${name.length}"}
        }
    }
}
