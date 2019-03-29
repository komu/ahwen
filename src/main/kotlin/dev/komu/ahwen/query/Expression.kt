package dev.komu.ahwen.query

import dev.komu.ahwen.record.Schema

sealed class Expression {

    fun asConstant(): Constant =
        (this as ConstantExpression).value

    fun asFieldName(): String=
        (this as FieldNameExpression).fieldName

    abstract fun evaluate(scan: Scan): Constant
    abstract fun appliesTo(schema: Schema): Boolean
    abstract override fun toString(): String
}

class ConstantExpression(val value: Constant) : Expression() {
    override fun evaluate(scan: Scan) = value
    override fun appliesTo(schema: Schema) = true
    override fun toString() = value.toString()
}

class FieldNameExpression(val fieldName: String) : Expression() {
    override fun evaluate(scan: Scan) = scan.getVal(fieldName)
    override fun appliesTo(schema: Schema) = schema.hasField(fieldName)
    override fun toString() = fieldName
}
