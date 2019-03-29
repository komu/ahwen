package dev.komu.ahwen.query

import dev.komu.ahwen.record.Schema

class ProjectPlan(private val plan: Plan, private val fields: Collection<String>) : Plan by plan {

    override val schema = Schema().also { schema ->
        val sch = plan.schema
        for (field in fields)
            schema.add(field, sch)
    }

    override fun open(): Scan =
        ProjectScan(plan.open(), fields)
}