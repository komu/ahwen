package dev.komu.ahwen.materialize

import dev.komu.ahwen.query.Plan
import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.query.UpdateScan
import dev.komu.ahwen.query.insertRowFrom
import dev.komu.ahwen.tx.Transaction

class SortPlan(
    private val plan: Plan,
    sortFields: List<String>,
    private val tx: Transaction
) : Plan {

    override val schema = plan.schema
    private val comparator = RecordComparator(sortFields)

    override fun open(): SortScan {
        var runs = plan.open().use { src ->
            splitIntoRuns(src)
        }

        while (runs.size > 2)
            runs = doMergeIteration(runs)

        return SortScan(runs, comparator)
    }

    override val blocksAccessed: Int
        get() = MaterializePlan(plan, tx).blocksAccessed

    override val recordsOutput: Int
        get() = plan.recordsOutput

    override fun distinctValues(fieldName: String): Int =
        plan.distinctValues(fieldName)

    private fun splitIntoRuns(src: Scan): MutableList<TempTable> {
        val temps = mutableListOf<TempTable>()

        src.beforeFirst()
        if (!src.next())
            return temps

        var currentTemp = TempTable(schema, tx)
        temps += currentTemp

        var currentScan = currentTemp.open()
        while (copy(src, currentScan)) {
            if (comparator.compare(src, currentScan) < 0) {
                currentScan.close()
                currentTemp = TempTable(schema, tx)
                temps += currentTemp
                currentScan = currentTemp.open()
            }
        }
        currentScan.close()
        return temps
    }

    private fun doMergeIteration(runs: MutableList<TempTable>): MutableList<TempTable> {
        val result = mutableListOf<TempTable>()

        while (runs.size > 1) {
            val (p1, p2) = runs
            runs.subList(0, 2).clear()
            result += mergeTwoRuns(p1, p2)
        }

        if (runs.size == 1)
            result += runs.first()

        return result
    }

    private fun mergeTwoRuns(p1: TempTable, p2: TempTable): TempTable {
        val src1 = p1.open()
        val src2 = p2.open()
        val result = TempTable(schema, tx)
        val dest = result.open()

        var hasMore1 = src1.next()
        var hasMore2 = src2.next()
        while (hasMore1 && hasMore2) {
            if (comparator.compare(src1, src2) < 0)
                hasMore1 = copy(src1, dest)
            else
                hasMore2 = copy(src2, dest)
        }

        if (hasMore1) {
            while (hasMore1)
                hasMore1 = copy(src1, dest)
        } else {
            while (hasMore2)
                hasMore2 = copy(src2, dest)
        }

        src1.close()
        src2.close()
        dest.close()
        return result
    }

    private fun copy(src: Scan, dest: UpdateScan): Boolean {
        dest.insertRowFrom(src, schema)
        return src.next()
    }
}
