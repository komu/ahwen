package dev.komu.ahwen.query

interface Scan {
    fun beforeFirst()
    fun next(): Boolean
    fun close()

    fun getVal(fieldName: String): Constant
    fun getInt(fieldName: String): Int
    fun getString(fieldName: String): String
    fun hasField(fieldName: String): Boolean
}

