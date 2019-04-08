package dev.komu.ahwen.record

import dev.komu.ahwen.types.SqlType
import dev.komu.ahwen.types.SqlType.INTEGER
import dev.komu.ahwen.types.SqlType.VARCHAR

/**
 * Represents a logical schema of a relation: basically a mapping from names to types.
 *
 * @see TableInfo
 */
class Schema private constructor(private val info: Map<String, FieldInfo>) {

    val fields: Collection<String>
        get() = info.keys

    fun hasField(name: String) =
        name in info

    fun type(name: String) =
        lookup(name).type

    fun length(name: String) =
        lookup(name).length

    operator fun plus(rhs: Schema): Schema =
        Schema(info + rhs.info)

    private fun lookup(name: String) =
        info[name] ?: error("no field $name")

    private class FieldInfo(val type: SqlType, val length: Int)

    companion object {

        inline operator fun invoke(callback: Builder.() -> Unit): Schema =
            Builder().apply(callback).build()
    }

    class Builder {

        private val info = mutableMapOf<String, FieldInfo>()

        fun addField(name: String, type: SqlType, length: Int) {
            info[name] = FieldInfo(type, length)
        }

        fun intField(name: String) {
            addField(name, INTEGER, 0)
        }

        fun stringField(name: String, length: Int) {
            addField(name, VARCHAR, length)
        }

        fun copyFieldFrom(name: String, schema: Schema) {
            val info = schema.lookup(name)

            addField(name, info.type, info.length)
        }

        fun build(): Schema =
            Schema(info)
    }
}
