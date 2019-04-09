package dev.komu.ahwen.query

import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.types.ColumnName

class Predicate() {

    private val terms = mutableListOf<Term>()

    constructor(term: Term): this() {
        terms += term
    }

    fun conjoinWith(predicate: Predicate) {
        terms += predicate.terms
    }

    fun isSatisfied(scan: Scan): Boolean =
        terms.all { it.isSatisfied(scan) }

    fun reductionFactor(plan: Plan): Int {
        var factor = 1
        for (term in terms)
            factor *= term.reductionFactor(plan)
        return factor
    }

    fun selectPredicate(schema: Schema): Predicate? {
        val result = Predicate()
        terms.filterTo(result.terms) { it.appliesTo(schema) }

        return result.takeIf { it.terms.isNotEmpty() }
    }

    fun joinPredicate(schema1: Schema, schema2: Schema): Predicate? {
        val result = Predicate()
        val newSchema = schema1 + schema2

        for (term in terms)
            if (!term.appliesTo(schema1) && !term.appliesTo(schema2) && term.appliesTo(newSchema))
                result.terms += term

        return result.takeIf { it.terms.isNotEmpty() }
    }

    fun equatesWithConstant(fieldName: ColumnName): Constant? {
        for (term in terms) {
            val value = term.equatesWithConstant(fieldName)
            if (value != null)
                return value
        }
        return null
    }

    fun equatesWithField(fieldName: ColumnName): ColumnName? {
        for (term in terms) {
            val field = term.equatesWithField(fieldName)
            if (field != null)
                return field
        }
        return null
    }

    override fun toString(): String {
        if (terms.isEmpty()) return ""

        return terms.joinToString(" and ")
    }
}
