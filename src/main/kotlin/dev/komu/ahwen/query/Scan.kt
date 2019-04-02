package dev.komu.ahwen.query

import java.io.Closeable

interface Scan : Closeable {
    fun beforeFirst()
    fun next(): Boolean

    fun getVal(fieldName: String): Constant
    fun getInt(fieldName: String): Int = getVal(fieldName).value as Int
    fun getString(fieldName: String): String = getVal(fieldName).value as String

    fun hasField(fieldName: String): Boolean
}

inline fun Scan.forEach(func: () -> Unit) {
    while (next())
        func()
}
