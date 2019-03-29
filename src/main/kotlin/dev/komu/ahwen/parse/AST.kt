package dev.komu.ahwen.parse

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.Expression
import dev.komu.ahwen.query.Predicate
import dev.komu.ahwen.record.Schema

sealed class CommandData

class InsertData(val table: String, val fields: List<String>, val values: List<Constant>) : CommandData()

class DeleteData(val table: String, val predicate: Predicate) : CommandData()

class ModifyData(val table: String, val fieldName: String, val newValue: Expression, val predicate: Predicate) : CommandData()

class QueryData(val fields: List<String>, val tables: List<String>, val predicate: Predicate) : CommandData() {
    override fun toString(): String = buildString {
        append("select ")
        fields.joinTo(this, separator = ", ")
        append(" from ")
        tables.joinTo(this, separator = ", ")

        val predString = predicate.toString()
        if (predString.isNotEmpty())
            append(" where ").append(predString)
    }
}

class CreateTableData(val table: String, val schema: Schema) : CommandData()

class CreateViewData(val view: String, query: QueryData) : CommandData()

class CreateIndexData(val index: String, val table: String, val field: String) : CommandData()
