package dev.komu.ahwen.query

interface Scan {
    fun beforeFirst()
    fun next(): Boolean
    fun close()

    fun getVal(fieldName: String): Constant
    fun getInt(fieldName: String): Int = getVal(fieldName).value as Int
    fun getString(fieldName: String): String = getVal(fieldName).value as String

    fun hasField(fieldName: String): Boolean
}

