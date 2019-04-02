package dev.komu.ahwen.query

class ProductPlan(private val p1: Plan, private val p2: Plan) : Plan {

    override val schema = p1.schema + p2.schema

    override fun open(): Scan =
        ProductScan(p1.open(), p2.open())

    override val blocksAccessed: Int
        get() {
            val blocks1 = p1.blocksAccessed
            val blocks2 = p2.blocksAccessed
            return blocks1 + (blocks1 * blocks2)
        }

    override val recordsOutput: Int
        get() = p1.recordsOutput * p2.recordsOutput

    override fun distinctValues(fieldName: String) =
        if (p1.schema.hasField(fieldName))
            p1.distinctValues(fieldName)
        else
            p2.distinctValues(fieldName)
}
