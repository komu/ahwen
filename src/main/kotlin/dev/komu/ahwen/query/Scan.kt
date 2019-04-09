package dev.komu.ahwen.query

import dev.komu.ahwen.types.ColumnName
import java.io.Closeable

/**
 * Represents an operator of relational algebra.
 *
 * Each supported operator of will have a one or more [Scan]-implementations that perform
 * the functionality of the operator. In theory only one implementation per operator would
 * be enough, but since different implementations have different performance characteristics,
 * its useful to have several (e.g. the optimal way to perform a join depends on several
 * factors).
 *
 * For each [Scan], there is a corresponding [Plan], then can be used to reason about the
 * performance characteristics of the scan without actually executing it.
 *
 * Some scans support updating the scanned records; those will implement [UpdateScan].
 */
interface Scan : Closeable {

    /**
     * Positions the scan to the very beginning.
     *
     * Each scan starts its state at the beginning, but this can be used to restart the
     * sequence. (E.g. [ProductScan] that performs a nested-loops join, needs to restart
     * its other child for every iteration of the outer loop.)
     */
    fun beforeFirst()

    /**
     * Moves to next row, if one exists.
     *
     * @return `true` if a new row was available, `false` if end has been reached
     */
    fun next(): Boolean

    /**
     * Returns the value of given column in current row.
     */
    operator fun get(column: ColumnName): Constant

    /**
     * Does the scan contain given column?
     */
    operator fun contains(column: ColumnName): Boolean
}

/**
 * Iterate over all rows of the scan.
 */
inline fun Scan.forEach(func: () -> Unit) {
    while (next())
        func()
}
