package dev.komu.ahwen.query

import dev.komu.ahwen.record.Schema

interface Plan {
    fun open(): Scan
    val blocksAccessed: Int
    val recordsOutput: Int
    fun distinctValues(fieldName: String): Int
    val schema: Schema
}
