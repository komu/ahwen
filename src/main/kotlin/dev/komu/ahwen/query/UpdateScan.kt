package dev.komu.ahwen.query

import dev.komu.ahwen.record.RID
import dev.komu.ahwen.record.Schema

interface UpdateScan : Scan {
    fun setVal(fieldName: String, value: Constant)
    fun setInt(fieldName: String, value: Int)
    fun setString(fieldName: String, value: String)

    fun insert()
    fun delete()

    val rid: RID
    fun moveToRid(rid: RID)
}

fun UpdateScan.copyFrom(src: Scan, schema: Schema) {
    while (src.next())
        insertRowFrom(src, schema)
}

fun UpdateScan.insertRowFrom(src: Scan, schema: Schema) {
    insert()
    for (field in schema.fields)
        setVal(field, src.getVal(field))
}
