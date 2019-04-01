package dev.komu.ahwen.index

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.record.RID

interface Index {
    fun beforeFirst(searchKey: Constant)
    fun next(): Boolean
    val dataRid: RID
    fun insert(dataVal: Constant, dataRid: RID)
    fun delete(dataVal: Constant, dataRid: RID)
    fun close()
}
