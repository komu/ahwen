package dev.komu.ahwen.query.materialize

import dev.komu.ahwen.query.Scan
import dev.komu.ahwen.types.ColumnName

class RecordComparator(private val fields: List<ColumnName>) : Comparator<Scan> {

    override fun compare(o1: Scan, o2: Scan): Int {
        for (field in fields) {
            val val1 = o1[field]
            val val2 = o2[field]
            val result = val1.compareTo(val2)
            if (result != 0)
                return result
        }
        return 0
    }
}
