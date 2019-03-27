package dev.komu.ahwen.query

interface Constant : Comparable<Constant> {

    fun asJavaValue(): Any
}

data class IntConstant(private val value: Int) : Constant {
    override fun asJavaValue(): Any = value

    override fun compareTo(other: Constant): Int =
        value.compareTo((other as IntConstant).value)

    override fun toString(): String = value.toString()
}

data class StringConstant(private val value: String) : Constant {
    override fun asJavaValue(): Any = value

    override fun compareTo(other: Constant): Int =
        value.compareTo((other as StringConstant).value)

    override fun toString(): String = value
}
