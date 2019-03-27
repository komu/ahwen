package dev.komu.ahwen.query

import dev.komu.ahwen.record.RID

class SelectScan(private val scan: Scan, private val predicate: Predicate) : Scan by scan, UpdateScan {

    override fun next(): Boolean {
        while (scan.next())
            if (predicate.isSatisfied(scan))
                return true
        return false
    }

    override fun setVal(fieldName: String, value: Constant) {
        val us = scan as UpdateScan
        us.setVal(fieldName, value)
    }

    override fun setInt(fieldName: String, value: Int) {
        val us = scan as UpdateScan
        us.setInt(fieldName, value)
    }

    override fun setString(fieldName: String, value: String) {
        val us = scan as UpdateScan
        us.setString(fieldName, value)
    }

    override fun delete() {
        val us = scan as UpdateScan
        us.delete()
    }

    override fun insert() {
        val us = scan as UpdateScan
        us.insert()
    }

    override val rid: RID
        get() = (scan as UpdateScan).rid

    override fun moveToRid(rid: RID) {
        val us = scan as UpdateScan
        us.moveToRid(rid)
    }
}
