package dev.komu.ahwen.query

import dev.komu.ahwen.record.RID

interface UpdateScan : Scan {
    fun setVal(fieldName: String, value: Constant)
    fun setInt(fieldName: String, value: Int)
    fun setString(fieldName: String, value: String)

    fun insert()
    fun delete()

    val rid: RID
    fun moveToRid(rid: RID)
}
