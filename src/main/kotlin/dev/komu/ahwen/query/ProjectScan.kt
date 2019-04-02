package dev.komu.ahwen.query

class ProjectScan(private val scan: Scan, private val fieldList: Collection<String>) : Scan {

    override fun getVal(fieldName: String): Constant =
        if (hasField(fieldName))
            scan.getVal(fieldName)
        else
            error("field not found $fieldName")

    override fun hasField(fieldName: String): Boolean =
        fieldName in fieldList

    override fun beforeFirst() {
        scan.beforeFirst()
    }

    override fun next(): Boolean =
        scan.next()

    override fun close() {
        scan.close()
    }
}
