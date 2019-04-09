package dev.komu.ahwen.query

import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.types.ColumnName

class ProjectPlan(private val plan: Plan, private val fields: Collection<ColumnName>) : Plan by plan {

    override val schema = Schema {
        val sch = plan.schema
        for (field in fields)
            copyFieldFrom(field, sch)
    }

    override fun open(): Scan =
        ProjectScan(plan.open(), fields)
}
