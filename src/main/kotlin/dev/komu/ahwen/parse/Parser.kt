package dev.komu.ahwen.parse

import dev.komu.ahwen.query.*
import dev.komu.ahwen.record.Schema

class Parser(private val lex: Lexer) {

    // Methods for parsing predicates, terms, etc.

    fun field(): String =
        lex.eatId()

    fun constant(): Constant =
        if (lex.matchStringConstant())
            StringConstant(lex.eatStringConstant())
        else
            IntConstant(lex.eatIntConstant())

    fun expression(): Expression =
        if (lex.matchId())
            FieldNameExpression(lex.eatId())
        else
            ConstantExpression(constant())

    fun term(): Term {
        val lhs = expression()
        lex.eatDelim('=')
        val rhs = expression()
        return Term(lhs, rhs)
    }

    fun predicate(): Predicate {
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

        return QueryData(fields, tables, predicate)
    }

    private fun selectList(): List<String> {
        val result = mutableListOf<String>()
        result += field()
        while (lex.matchDelim(',')) {
            lex.eatDelim(',')
            result += field()
        }
        return result
    }

    private fun tableList(): List<String> {
        val result = mutableListOf<String>()
        result += lex.eatId()
        while (lex.matchDelim(',')) {
            lex.eatDelim(',')
            result += lex.eatId()
        }
        return result
    }

    // Methods for parsing update commands
    fun updateCmd(): CommandData = when {
        lex.matchKeyword("insert") ->
            insert()
        lex.matchKeyword("delete") ->
            delete()
        lex.matchKeyword("update") ->
            modify()
        else ->
            create()
    }

    fun create(): CommandData {
        lex.eatKeyword("create")
        return when {
            lex.matchKeyword("table") -> createTable()
            lex.matchKeyword("view") -> createView()
            else -> createIndex()
        }
    }

    fun delete(): DeleteData {
        lex.eatKeyword("delete")
        lex.eatKeyword("from")
        val table = lex.eatId()
        val predicate = if (lex.matchKeyword("where")) {
            lex.eatKeyword("where")
            predicate()
        } else {
            Predicate()
        }
        return DeleteData(table, predicate)
    }

    fun insert(): InsertData {
        lex.eatKeyword("insert")
        lex.eatKeyword("into")
        val table = lex.eatId()
        lex.eatDelim('(')
        val fields = fieldList()
        lex.eatDelim(')')
        lex.eatKeyword("values")
        lex.eatDelim('(')
        val values = constList()
        lex.eatDelim(')')

        return InsertData(table, fields, values)
    }

    private fun fieldList(): List<String> {
        val fields = mutableListOf<String>()
        fields += field()
        while (lex.matchDelim(',')) {
            lex.eatDelim(',')
            fields += field()
        }
        return fields
    }

    private fun constList(): List<Constant> {
        val values = mutableListOf<Constant>()
        values += constant()
        while (lex.matchDelim(',')) {
            lex.eatDelim(',')
            values += constant()
        }
        return values
    }

    fun modify(): ModifyData {
        lex.eatKeyword("update")
        val table = lex.eatId()
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
        val name = lex.eatId()
        lex.eatDelim('(')
        val schema = fieldDefs()
        lex.eatDelim(')')
        return CreateTableData(name, schema)
    }

    private fun fieldDefs(): Schema {
        val schema = fieldDef()
        while (lex.matchDelim(',')) {
            lex.eatDelim(',')
            schema.addAll(fieldDef())
        }
        return schema
    }

    private fun fieldDef(): Schema {
        val fieldName = field()
        return fieldType(fieldName)
    }

    private fun fieldType(fieldName: String): Schema {
        val schema = Schema()
        if (lex.matchKeyword("int")) {
            lex.eatKeyword("int")
            schema.addIntField(fieldName)
        } else {
            lex.eatKeyword("varchar")
            lex.eatDelim('(')
            val len = lex.eatIntConstant()
            lex.eatDelim(')')
            schema.addStringField(fieldName, len)
        }
        return schema
    }

    private fun createView(): CreateViewData {
        lex.eatKeyword("view")
        val viewName = lex.eatId()
        lex.eatKeyword("as")
        val query = query()
        return CreateViewData(viewName, query)
    }

    private fun createIndex(): CreateIndexData {
        lex.eatKeyword("index")
        val index = lex.eatId()
        lex.eatKeyword("on")
        val table = lex.eatId()
        lex.eatDelim('(')
        val field = field()
        lex.eatDelim(')')
        return CreateIndexData(index, table, field)
    }
}
