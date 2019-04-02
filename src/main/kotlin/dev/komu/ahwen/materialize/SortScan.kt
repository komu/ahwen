package dev.komu.ahwen.materialize

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.record.RID

class SortScan(
    runs: List<TempTable>,
    private val comparator: RecordComparator
) : Scan {

    private val s1 = runs[0].open()
    private var s2 = runs.getOrNull(1)?.open()
    private var hasMore1 = s1.next()
    private var hasMore2 = s2?.next() ?: false
    private var currentScan: Scan? = null
    private var savedPosition: Pair<RID, RID?>? = null

    override fun beforeFirst() {
        currentScan = null
        s1.beforeFirst()
        hasMore1 = s1.next()
        s2?.let { s2 ->
            s2.beforeFirst()
            hasMore2 = s2.next()
        }
    }

    override fun next(): Boolean {
        currentScan?.let { currentScan ->
            if (currentScan == s1)
                hasMore1 = currentScan.next()
            else if (currentScan == s2)
                hasMore2 = currentScan.next()
        }

        if (!hasMore1 && !hasMore2)
            return false

        currentScan = when {
            hasMore1 && hasMore2 -> minOf(s1, s2!!, comparator)
            hasMore1 -> s1
            else -> s2
        }

        return true
    }

    override fun close() {
        s1.close()
        s2?.close()
    }

    override fun getVal(fieldName: String): Constant =
        currentScan!!.getVal(fieldName)

    override fun hasField(fieldName: String) =
        currentScan!!.hasField(fieldName)

    fun savePosition() {
        savedPosition = Pair(s1.rid, s2?.rid)
    }

    fun restorePosition() {
        val (rid1, rid2) = savedPosition ?: error("no saved position")
        s1.moveToRid(rid1)
        if (rid2 != null)
            s2!!.moveToRid(rid2)
    }
}
