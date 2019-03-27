package dev.komu.ahwen.metadata

class StatInfo(val numBlocks: Int, val numRecords: Int) {

    fun distinctValues(fieldName: String): Int =
        1 + (numRecords / 3)
}
