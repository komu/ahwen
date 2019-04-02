package dev.komu.ahwen.query

class ProductScan(private val s1: Scan, private val s2: Scan) : Scan {
    init {
        s1.next()
    }

    override fun beforeFirst() {
        s1.beforeFirst()
        s1.next()
        s2.beforeFirst()
    }

    override fun next() =
        if (s2.next()) {
            true
        } else {
            s2.beforeFirst()
            s2.next() && s1.next()
        }

    override fun close() {
        s1.close()
        s2.close()
    }

    override fun getVal(fieldName: String): Constant =
        if (s1.hasField(fieldName)) s1.getVal(fieldName) else s2.getVal(fieldName)

    override fun hasField(fieldName: String): Boolean =
        s1.hasField(fieldName) || s2.hasField(fieldName)
}
