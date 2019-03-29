package dev.komu.ahwen.query

sealed class Constant : Comparable<Constant> {
    abstract val value: Any
}

data class IntConstant(override val value: Int) : Constant() {

    override fun compareTo(other: Constant): Int =
        value.compareTo((other as IntConstant).value)

    override fun toString(): String = value.toString()
}

data class StringConstant(override val value: String) : Constant() {
    override fun compareTo(other: Constant): Int =
        value.compareTo((other as StringConstant).value)

    override fun toString(): String = value
}
