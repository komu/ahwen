package dev.komu.ahwen.metadata

import dev.komu.ahwen.metadata.TableManager.Companion.checkNameLength
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction

/**
 * Class responsible for maintaining views.
 */
class ViewManager(isNew: Boolean, private val tableManager: TableManager, tx: Transaction) {

    init {
        if (isNew) {
            val schema = Schema {
                stringField(COL_VIEW_NAME, TableManager.MAX_NAME)
                stringField(COL_VIEW_DEF, MAX_VIEWDEF)
            }
            tableManager.createTable(TBL_VIEW_CAT, schema, tx)
        }
    }

    fun createView(name: String, def: String, tx: Transaction) {
        checkNameLength(name, "name")
        tableManager.getTableInfo(TBL_VIEW_CAT, tx).open(tx).use { rf ->
            rf.insert()
            rf.setString(COL_VIEW_NAME, name)
            rf.setString(COL_VIEW_DEF, def)
        }
    }

    fun getViewDef(name: String, tx: Transaction): String? {
        tableManager.getTableInfo(TBL_VIEW_CAT, tx).open(tx).use { rf ->
            var result: String? = null
            while (rf.next()) {
                if (rf.getString(COL_VIEW_NAME) == name) {
                    result = rf.getString(COL_VIEW_DEF)
                    break
                }
            }
            return result
        }
    }

    companion object {

        private const val TBL_VIEW_CAT = "viewcat"
        private const val COL_VIEW_NAME = "viewname"
        private const val COL_VIEW_DEF = "viewdef"

        private const val MAX_VIEWDEF = 100
    }
}
