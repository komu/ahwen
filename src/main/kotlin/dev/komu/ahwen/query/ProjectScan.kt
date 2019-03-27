package dev.komu.ahwen.query

class ProjectScan(private val scan: Scan, private val fieldList: Collection<String>) : Scan by scan {

    override fun getVal(fieldName: String): Constant =
        if (hasField(fieldName))
            scan.getVal(fieldName)
        else
            error("field not found $fieldName")

    override fun getInt(fieldName: String): Int =
        if (hasField(fieldName))
            scan.getInt(fieldName)
        else
            error("field not found $fieldName")

    override fun getString(fieldName: String): String =
        if (hasField(fieldName))
            scan.getString(fieldName)
        else
            error("field not found $fieldName")

    override fun hasField(fieldName: String): Boolean =
        fieldName in fieldList
}
