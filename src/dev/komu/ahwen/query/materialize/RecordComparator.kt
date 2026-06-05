package dev.komu.ahwen.query.materialize

import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

/**
 * Comparator that compares current rows of scans using given column names.
 */
class RecordComparator(private val columns: List<ColumnName>) : Comparator<Scan> {

    override fun compare(o1: Scan, o2: Scan): Int {
        for (field in columns) {
            val ordering = o1[field].compareTo(o2[field])
            if (ordering != 0)
                return ordering
        }

        return 0
    }
}
