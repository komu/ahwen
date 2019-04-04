package dev.komu.ahwen.query

import dev.komu.ahwen.record.RID
import dev.komu.ahwen.record.RecordFile
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.SqlType

class TableScan(ti: TableInfo, tx: Transaction) : UpdateScan {

    private val rf = RecordFile(ti, tx)
    private val schema = ti.schema

    override fun beforeFirst() {
        rf.beforeFirst()
    }

    override fun next(): Boolean =
        rf.next()

    override fun close() {
        rf.close()
    }

    override fun getVal(fieldName: String): Constant {
        val type = schema.type(fieldName)
        return when (type) {
            SqlType.INTEGER -> IntConstant(rf.getInt(fieldName))
            SqlType.VARCHAR -> StringConstant(rf.getString(fieldName))
        }
    }

    override fun getInt(fieldName: String): Int =
        rf.getInt(fieldName)

    override fun getString(fieldName: String): String =
        rf.getString(fieldName)

    override fun hasField(fieldName: String): Boolean =
        schema.hasField(fieldName)

    override fun setVal(fieldName: String, value: Constant) {
        val type = schema.type(fieldName)
        return when (type) {
            SqlType.INTEGER -> rf.setInt(fieldName, value.value as Int)
            SqlType.VARCHAR -> rf.setString(fieldName, value.value as String)
        }
    }

    override fun setInt(fieldName: String, value: Int) {
        rf.setInt(fieldName, value)
    }

    override fun setString(fieldName: String, value: String) {
        rf.setString(fieldName, value)
    }

    override fun delete() {
        rf.delete()
    }

    override fun insert() {
        rf.insert()
    }

    override val rid: RID
        get() = rf.currentRid

    override fun moveToRid(rid: RID) {
        rf.moveToRid(rid)
    }
}
