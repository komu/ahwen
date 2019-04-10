package dev.komu.ahwen.query.materialize

import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

/**
 * Runtime implementation of [MergeJoinPlan].
 */
class MergeJoinScan(
    private val s1: Scan,
    private val s2: SortScan,
    private val joinColumn1: ColumnName,
    private val joinColumn2: ColumnName
) : Scan {

    private var joinValue: SqlValue? = null

    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        s1.beforeFirst()
        s2.beforeFirst()
    }

    override fun close() {
        s1.close()
        s2.close()
    }

    override fun next(): Boolean {
        var hasMore2 = s2.next()
        if (hasMore2 && s2[joinColumn2] == joinValue)
            return true

        var hasMore1 = s1.next()
        if (hasMore1 && s1[joinColumn1] == joinValue) {
            s2.restorePosition()
            return true
        }

        while (hasMore1 && hasMore2) {
            val v1 = s1[joinColumn1]
            val v2 = s2[joinColumn2]
            when {
                v1 < v2 ->
                    hasMore1 = s1.next()
                v1 > v2 ->
                    hasMore2 = s2.next()
                else -> {
                    s2.savePosition()
                    joinValue = s2[joinColumn2]
                    return true
                }
            }
        }

        return false
    }

    override fun get(column: ColumnName): SqlValue =
        if (column in s1) s1[column] else s2[column]

    override fun contains(column: ColumnName): Boolean =
        column in s1 || column in s2
}
