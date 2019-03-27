package dev.komu.ahwen.metadata

import dev.komu.ahwen.record.RecordFile
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction

class ViewManager(isNew: Boolean, private val tableManager: TableManager, tx: Transaction) {

    init {
        if (isNew) {
            val schema = Schema()
            schema.addStringField("viewname", TableManager.MAX_NAME)
            schema.addStringField("viewdef", MAX_VIEWDEF)
            tableManager.createTable("viewcat", schema, tx)
        }
    }

    fun createView(name: String, def: String, tx: Transaction) {
        val ti = tableManager.getTableInfo("viewcat", tx)
        val rf = RecordFile(ti, tx)
        rf.insert()
        rf.setString("viewname", name)
        rf.setString("viewdef", def)
        rf.close()
    }

    fun getViewDef(name: String, tx: Transaction): String? {
        val ti = tableManager.getTableInfo("viewcat", tx)
        val rf = RecordFile(ti, tx)
        var result: String? = null
        while (rf.next()) {
            if (rf.getString("viewname") == name) {
                result = rf.getString("viewdef")
                break
            }
        }

        rf.close()
        return result
    }

    companion object {
        private const val MAX_VIEWDEF = 100
    }
}
