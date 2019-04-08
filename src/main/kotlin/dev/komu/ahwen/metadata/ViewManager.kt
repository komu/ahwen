package dev.komu.ahwen.metadata

import dev.komu.ahwen.metadata.TableManager.Companion.checkNameLength
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction

class ViewManager(isNew: Boolean, private val tableManager: TableManager, tx: Transaction) {

    init {
        if (isNew) {
            val schema = Schema {
                stringField("viewname", TableManager.MAX_NAME)
                stringField("viewdef", MAX_VIEWDEF)
            }
            tableManager.createTable("viewcat", schema, tx)
        }
    }

    fun createView(name: String, def: String, tx: Transaction) {
        checkNameLength(name, "name")
        tableManager.getTableInfo("viewcat", tx).open(tx).use { rf ->
            rf.insert()
            rf.setString("viewname", name)
            rf.setString("viewdef", def)
        }
    }

    fun getViewDef(name: String, tx: Transaction): String? {
        tableManager.getTableInfo("viewcat", tx).open(tx).use { rf ->
            var result: String? = null
            while (rf.next()) {
                if (rf.getString("viewname") == name) {
                    result = rf.getString("viewdef")
                    break
                }
            }
            return result
        }
    }

    companion object {
        private const val MAX_VIEWDEF = 100
    }
}
