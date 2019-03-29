package dev.komu.ahwen.query

import dev.komu.ahwen.record.Schema

class Term(private val lhs: Expression, private val rhs: Expression) {

    fun reductionFactor(plan: Plan): Int = when {
        lhs is FieldNameExpression && rhs is FieldNameExpression ->
            maxOf(plan.distinctValues(lhs.fieldName), plan.distinctValues(rhs.fieldName))
        lhs is FieldNameExpression ->
            plan.distinctValues(lhs.fieldName)
        rhs is FieldNameExpression ->
            plan.distinctValues(rhs.fieldName)
        lhs.asConstant() == rhs.asConstant() ->
            1
        else ->
            Integer.MAX_VALUE
    }

    fun equatesWithConstant(fieldName: String): Constant? = when {
        lhs is FieldNameExpression && rhs is ConstantExpression && lhs.fieldName == fieldName ->
            rhs.value
        rhs is FieldNameExpression && lhs is ConstantExpression && rhs.fieldName == fieldName ->
            lhs.value
        else ->
            null
    }

    fun equatesWithField(fieldName: String): String? = when {
        lhs is FieldNameExpression && rhs is FieldNameExpression && lhs.fieldName == fieldName ->
            rhs.fieldName
        lhs is FieldNameExpression && rhs is FieldNameExpression && rhs.fieldName == fieldName ->
            lhs.fieldName
        else ->
            null
    }

    fun appliesTo(schema: Schema): Boolean =
        lhs.appliesTo(schema) && rhs.appliesTo(schema)

    fun isSatisfied(scan: Scan): Boolean =
        lhs.evaluate(scan) == rhs.evaluate(scan)

    override fun toString(): String =
        "$lhs=$rhs"
}
