package dev.komu.ahwen.planner

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.query.index.IndexJoinPlan
import dev.komu.ahwen.query.index.IndexSelectPlan
import dev.komu.ahwen.metadata.MetadataManager
import dev.komu.ahwen.query.*
import dev.komu.ahwen.query.materialize.MultiBufferProductPlan
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.TableName

/**
 * Planner that tries to perform optimal selects, joins or products for given table,
 * using indices if possible.
 */
class TablePlanner(
    tableName: TableName,
    private val myPredicate: Predicate,
    private val tx: Transaction,
    metadataManager: MetadataManager,
    private val bufferManager: BufferManager
) {

    private val myPlan = TablePlan(tableName, metadataManager, tx)
    private val mySchema = myPlan.schema
    private val indices = metadataManager.getIndexInfo(tableName, tx)

    fun makeSelectPlan(): Plan =
        addSelectPredicate(makeIndexSelect() ?: myPlan)

    fun makeJoinPlan(current: Plan): Plan? {
        val currentSchema = current.schema
        if (myPredicate.joinPredicate(mySchema, currentSchema) == null)
            return null

        return makeIndexJoin(current, currentSchema) ?: makeProductJoin(current, currentSchema)
    }

    fun makeProductPlan(current: Plan): Plan {
        val plan = addSelectPredicate(myPlan)
        return MultiBufferProductPlan(current, plan, tx, bufferManager)
    }

    private fun makeIndexSelect(): Plan? {
        for ((field, indexInfo) in indices) {
            val value = myPredicate.equatesWithConstant(field)
            if (value != null)
                return IndexSelectPlan(myPlan, indexInfo, value)
        }
        return null
    }

    private fun makeIndexJoin(current: Plan, currentSchema: Schema): Plan? {
        for ((field, indexInfo) in indices) {
            val outerField = myPredicate.equatesWithField(field)
            if (outerField != null && outerField in currentSchema) {
                val plan = addSelectPredicate(IndexJoinPlan(current, myPlan, indexInfo, outerField))
                return addJoinPredicate(plan, currentSchema)
            }
        }
        return null
    }

    private fun makeProductJoin(current: Plan, currentSchema: Schema): Plan =
        addJoinPredicate(makeProductPlan(current), currentSchema)

    private fun addSelectPredicate(plan: Plan): Plan {
        val predicate = myPredicate.selectPredicate(mySchema)
        return if (predicate != null) SelectPlan(plan, predicate) else plan
    }

    private fun addJoinPredicate(plan: Plan, currentSchema: Schema): Plan {
        val predicate = myPredicate.joinPredicate(currentSchema, mySchema)
        return if (predicate != null) SelectPlan(plan, predicate) else plan
    }
}
