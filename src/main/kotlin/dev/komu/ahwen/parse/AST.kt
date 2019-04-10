package dev.komu.ahwen.parse

import dev.komu.ahwen.query.Expression
import dev.komu.ahwen.query.Predicate
import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.IndexName
import dev.komu.ahwen.types.TableName

sealed class CommandData

class InsertData(val table: TableName, val fields: List<ColumnName>, val values: List<SqlValue>) : CommandData()

class DeleteData(val table: TableName, val predicate: Predicate) : CommandData()

class ModifyData(val table: TableName, val fieldName: ColumnName, val newValue: Expression, val predicate: Predicate) : CommandData()

class QueryData(
    val selectExps: List<SelectExp>,
    val tables: List<TableName>,
    val predicate: Predicate,
    val groupBy: List<ColumnName>,
    val orderBy: List<ColumnName>
    ) {
    override fun toString(): String = buildString {
        append("select ")
        selectExps.joinTo(this, separator = ", ")
        append(" from ")
        tables.joinTo(this, separator = ", ")

        val predString = predicate.toString()
        if (predString.isNotEmpty())
            append(" where ").append(predString)

        if (orderBy.isNotEmpty()) {
            append(" order by ")
            orderBy.joinTo(this, separator = ", ")
        }
    }
}

sealed class SelectExp {
    class Column(val column: ColumnName) : SelectExp() {
        override fun toString() = column.toString()
    }

    class Aggregate(val fn: String, val column: ColumnName) : SelectExp() {
        override fun toString() = "$fn($column)"
    }
}

class CreateTableData(val table: TableName, val schema: Schema) : CommandData()

class CreateViewData(val view: TableName, private val query: QueryData) : CommandData() {
    val viewDefinition: String
        get() = query.toString()
}

class CreateIndexData(val index: IndexName, val table: TableName, val field: ColumnName) : CommandData()
