package dev.komu.ahwen.record

import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.SqlType
import dev.komu.ahwen.types.SqlType.INTEGER
import dev.komu.ahwen.types.SqlType.VARCHAR

/**
 * Represents a logical schema of a relation: basically a mapping from names to types.
 *
 * @see TableInfo
 */
class Schema private constructor(private val info: Map<ColumnName, FieldInfo>) {

    val fields: Collection<ColumnName>
        get() = info.keys

    operator fun contains(name: ColumnName) =
        name in info

    fun type(name: ColumnName) =
        lookup(name).type

    fun length(name: ColumnName) =
        lookup(name).length

    operator fun plus(rhs: Schema): Schema =
        Schema(info + rhs.info)

    private fun lookup(name: ColumnName) =
        info[name] ?: error("no field $name")

    private class FieldInfo(val type: SqlType, val length: Int)

    companion object {

        inline operator fun invoke(callback: Builder.() -> Unit): Schema =
            Builder().apply(callback).build()
    }

    class Builder {

        private val info = mutableMapOf<ColumnName, FieldInfo>()

        fun addField(name: ColumnName, type: SqlType, length: Int) {
            info[name] = FieldInfo(type, length)
        }

        fun intField(name: ColumnName) {
            addField(name, INTEGER, 0)
        }

        fun stringField(name: ColumnName, length: Int) {
            addField(name, VARCHAR, length)
        }

        fun copyFieldFrom(name: ColumnName, schema: Schema) {
            val info = schema.lookup(name)

            addField(name, info.type, info.length)
        }

        fun build(): Schema =
            Schema(info)
    }
}
