package dev.komu.ahwen.index

import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.record.RID
import java.io.Closeable

/**
 * Cursor for indices.
 */
interface Index : Closeable {

    /**
     * Positions the cursor before given search value.
     */
    fun beforeFirst(searchKey: SqlValue)

    /**
     * Positions the cursor to next value. Returns `false` is there are no values left.
     */
    fun next(): Boolean

    /**
     * Returns the row id of current value
     */
    val dataRid: RID

    /**
     * Inserts a new value to this index.
     */
    fun insert(dataVal: SqlValue, dataRid: RID)

    /**
     * Deletes a value from this index.
     */
    fun delete(dataVal: SqlValue, dataRid: RID)
}
