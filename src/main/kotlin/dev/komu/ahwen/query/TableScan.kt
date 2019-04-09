package dev.komu.ahwen.query

import dev.komu.ahwen.record.RID
import dev.komu.ahwen.record.TableInfo
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.SqlType.INTEGER
import dev.komu.ahwen.types.SqlType.VARCHAR

class TableScan(ti: TableInfo, tx: Transaction) : UpdateScan {

    private val rf = ti.open(tx)
    private val schema = ti.schema

    override fun beforeFirst() {
        rf.beforeFirst()
    }

    override fun next(): Boolean =
        rf.next()

    override fun close() {
        rf.close()
    }

    override fun get(column: ColumnName) = when (schema.type(column)) {
        INTEGER -> IntConstant(rf.getInt(column))
        VARCHAR -> StringConstant(rf.getString(column))
    }

    override fun contains(column: ColumnName): Boolean =
        column in schema

    override fun set(column: ColumnName, value: Constant) = when (schema.type(column)) {
        INTEGER -> rf.setInt(column, value.value as Int)
        VARCHAR -> rf.setString(column, value.value as String)
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
