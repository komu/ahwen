package dev.komu.ahwen.query

import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.types.ColumnName

sealed class Expression {

    abstract fun evaluate(scan: Scan): Constant
    abstract fun appliesTo(schema: Schema): Boolean
    abstract override fun toString(): String
}

class ConstantExpression(val value: Constant) : Expression() {
    override fun evaluate(scan: Scan) = value
    override fun appliesTo(schema: Schema) = true
    override fun toString() = value.toString()
}

class FieldNameExpression(val fieldName: ColumnName) : Expression() {
    override fun evaluate(scan: Scan) = scan[fieldName]
    override fun appliesTo(schema: Schema) = fieldName in schema
    override fun toString() = fieldName.value
}
