package dev.komu.ahwen.query

import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.types.ColumnName

/**
 * Represents a node in a query plan.
 *
 * Planner takes AST created from SQL statements and creates various candidate plans
 * and compares them when trying to build an optimal plan tree. When the planner
 * has decided on plan, it will be _opened_, creating a respective tree of [Scan]s.
 */
interface Plan {

    /** Schema for the results returned by this plan */
    val schema: Schema

    /** Estimated amount of blocks that need to be loaded to execute this plan */
    val blocksAccessed: Int

    /** Estimated amount of records returned by execution */
    val recordsOutput: Int

    /** How many distinct values given column is estimated to have? */
    fun distinctValues(column: ColumnName): Int

    /**
     * Executes a plan, returning a [Scan] that performs the work that the plan represents.
     */
    fun open(): Scan
}
