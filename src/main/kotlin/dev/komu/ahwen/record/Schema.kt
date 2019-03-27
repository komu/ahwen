package dev.komu.ahwen.record

import java.sql.Types.INTEGER
import java.sql.Types.VARCHAR

class Schema {

    private val info = mutableMapOf<String, FieldInfo>()

    fun addField(name: String, type: Int, length: Int) {
        info[name] = FieldInfo(type, length)
    }

    fun addIntField(name: String) {
        addField(name, INTEGER, 0)
    }

    fun addStringField(name: String, length: Int) {
        addField(name, VARCHAR, length)
    }

    fun add(name: String, schema: Schema) {
        val info = schema.lookup(name)

        addField(name, info.type, info.length)
    }

    fun addAll(schema: Schema) {
        info.putAll(schema.info)
    }

    val fields: Collection<String>
        get() = info.keys

    fun hasField(name: String) =
        name in info

    fun type(name: String) =
        lookup(name).type

    fun length(name: String) =
        lookup(name).length

    private fun lookup(name: String) =
        info[name] ?: error("no field $name")

    private class FieldInfo(val type: Int, val length: Int)
}
