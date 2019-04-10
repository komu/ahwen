package dev.komu.ahwen.query

import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.types.ColumnName

/**
 * Represents an SQL expression.
 *
 * Currently expressions are very simple: they can name columns or they can be constant values.
 */
sealed class Expression {

    abstract fun evaluate(scan: Scan): SqlValue
    abstract fun appliesTo(schema: Schema): Boolean
    abstract override fun toString(): String

    class Const(val value: SqlValue) : Expression() {
        override fun evaluate(scan: Scan) = value
        override fun appliesTo(schema: Schema) = true
        override fun toString(): String = value.toString()
    }

    class Column(val fieldName: ColumnName) : Expression() {
        override fun evaluate(scan: Scan) = scan[fieldName]
        override fun appliesTo(schema: Schema) = fieldName in schema
        override fun toString() = fieldName.value
    }
}
