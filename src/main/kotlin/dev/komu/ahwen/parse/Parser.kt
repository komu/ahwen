package dev.komu.ahwen.parse

import dev.komu.ahwen.query.*
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.IndexName
import dev.komu.ahwen.types.TableName

class Parser(query: String) {

    // Methods for parsing predicates, terms, etc.

    private val lex = Lexer(query)

    private fun field(): ColumnName =
        ColumnName(lex.eatId())

    private fun tableName(): TableName =
        TableName(lex.eatId())

    private fun indexName(): IndexName =
        IndexName(lex.eatId())

    private fun constant(): SqlValue =
        if (lex.matchStringConstant())
            SqlString(lex.eatStringConstant())
        else
            SqlInt(lex.eatIntConstant())

    private fun expression(): Expression =
        if (lex.matchId())
            Expression.Column(ColumnName(lex.eatId()))
        else
            Expression.Const(constant())

    private fun term(): Term {
        val lhs = expression()
        lex.eatDelim('=')
        val rhs = expression()
        return Term(lhs, rhs)
    }

    private fun predicate(): Predicate {
        val pred = Predicate(term())
        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and")
            pred.conjoinWith(predicate())
        }
        return pred
    }

    // Methods for parsing queries

    fun query(): QueryData {
        lex.eatKeyword("select")
        val fields = selectList()
        lex.eatKeyword("from")
        val tables = tableList()
        val predicate = if (lex.matchKeyword("where")) {
            lex.eatKeyword("where")
            predicate()
        } else {
            Predicate()
        }

        val orderBy = if (lex.matchKeyword("order")) {
            lex.eatKeyword("order")
            lex.eatKeyword("by")
            selectList()
        } else {
            emptyList()
        }

        lex.assertEnd()

        return QueryData(fields, tables, predicate, orderBy)
    }

    private fun selectList(): List<ColumnName> {
        val result = mutableListOf<ColumnName>()
        result += field()
        while (lex.matchDelim(',')) {
            lex.eatDelim(',')
            result += field()
        }
        return result
    }

    private fun tableList(): List<TableName> {
        val result = mutableListOf<TableName>()
        result += tableName()
        while (lex.matchDelim(',')) {
            lex.eatDelim(',')
            result += tableName()
        }
        return result
    }

    // Methods for parsing update commands
    fun updateCmd(): CommandData {
        val command = when {
            lex.matchKeyword("insert") ->
                insert()
            lex.matchKeyword("delete") ->
                delete()
            lex.matchKeyword("update") ->
                modify()
            else ->
                create()
        }
        lex.assertEnd()
        return command
    }

    private fun create(): CommandData {
        lex.eatKeyword("create")
        return when {
            lex.matchKeyword("table") -> createTable()
            lex.matchKeyword("view") -> createView()
            else -> createIndex()
        }
    }

    private fun delete(): DeleteData {
        lex.eatKeyword("delete")
        lex.eatKeyword("from")
        val table = tableName()
        val predicate = if (lex.matchKeyword("where")) {
            lex.eatKeyword("where")
            predicate()
        } else {
            Predicate()
        }
        return DeleteData(table, predicate)
    }

    private fun insert(): InsertData {
        lex.eatKeyword("insert")
        lex.eatKeyword("into")
        val table = tableName()
        lex.eatDelim('(')
        val fields = fieldList()
        lex.eatDelim(')')
        lex.eatKeyword("values")
        lex.eatDelim('(')
        val values = constList()
        lex.eatDelim(')')

        return InsertData(table, fields, values)
    }

    private fun fieldList(): List<ColumnName> {
        val fields = mutableListOf<ColumnName>()
        fields += field()
        while (lex.matchDelim(',')) {
            lex.eatDelim(',')
            fields += field()
        }
        return fields
    }

    private fun constList(): List<SqlValue> {
        val values = mutableListOf<SqlValue>()
        values += constant()
        while (lex.matchDelim(',')) {
            lex.eatDelim(',')
            values += constant()
        }
        return values
    }

    private fun modify(): ModifyData {
        lex.eatKeyword("update")
        val table = tableName()
        lex.eatKeyword("set")
        val field = field()
        lex.eatDelim('=')
        val newValue = expression()

        val predicate = if (lex.matchKeyword("where")) {
            lex.eatKeyword("where")
            predicate()
        } else {
            Predicate()
        }

        return ModifyData(table, field, newValue, predicate)
    }

    private fun createTable(): CreateTableData {
        lex.eatKeyword("table")
        val name = tableName()
        lex.eatDelim('(')
        val schema = fieldDefs()
        lex.eatDelim(')')
        return CreateTableData(name, schema)
    }

    private fun fieldDefs(): Schema {
        var schema = fieldDef()
        while (lex.matchDelim(',')) {
            lex.eatDelim(',')
            schema += fieldDef()
        }
        return schema
    }

    private fun fieldDef(): Schema {
        val fieldName = field()
        return fieldType(fieldName)
    }

    private fun fieldType(fieldName: ColumnName): Schema {
        return if (lex.matchKeyword("int")) {
            lex.eatKeyword("int")
            Schema {
                intField(fieldName)
            }
        } else {
            lex.eatKeyword("varchar")
            lex.eatDelim('(')
            val len = lex.eatIntConstant()
            lex.eatDelim(')')
            Schema {
                stringField(fieldName, len)
            }
        }
    }

    private fun createView(): CreateViewData {
        lex.eatKeyword("view")
        val viewName = tableName()
        lex.eatKeyword("as")
        val query = query()
        return CreateViewData(viewName, query)
    }

    private fun createIndex(): CreateIndexData {
        lex.eatKeyword("index")
        val index = indexName()
        lex.eatKeyword("on")
        val table = tableName()
        lex.eatDelim('(')
        val field = field()
        lex.eatDelim(')')
        return CreateIndexData(index, table, field)
    }
}
