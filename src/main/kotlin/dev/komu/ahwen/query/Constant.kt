package dev.komu.ahwen.query

import dev.komu.ahwen.file.Page.Companion.strSize
import dev.komu.ahwen.types.SqlType

sealed class Constant : Comparable<Constant> {
    abstract val value: Any
    abstract val type: SqlType
    abstract val representationSize: Int
}

data class IntConstant(override val value: Int) : Constant() {

    override val type: SqlType
        get() = SqlType.INTEGER

    override val representationSize: Int
        get() = Int.SIZE_BYTES

    override fun compareTo(other: Constant): Int =
        value.compareTo((other as IntConstant).value)

    override fun toString(): String = value.toString()
}

data class StringConstant(override val value: String) : Constant() {

    override val type: SqlType
        get() = SqlType.VARCHAR

    override val representationSize: Int
        get() = strSize(value.length)

    override fun compareTo(other: Constant): Int =
        value.compareTo((other as StringConstant).value)

    override fun toString(): String = value
}
