package dev.komu.ahwen.query

class Predicate {
    fun isSatisfied(scan: Scan): Boolean {
        TODO()
    }

    fun reductionFactor(plan: Plan): Int {
        TODO()
    }

    fun equatesWithConstant(fieldName: String): Constant? {
        TODO()
    }
}
