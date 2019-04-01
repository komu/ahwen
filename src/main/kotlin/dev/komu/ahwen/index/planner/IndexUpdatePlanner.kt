package dev.komu.ahwen.index.planner

import dev.komu.ahwen.metadata.MetadataManager
import dev.komu.ahwen.parse.*
import dev.komu.ahwen.planner.UpdatePlanner
import dev.komu.ahwen.query.SelectPlan
import dev.komu.ahwen.query.TablePlan
import dev.komu.ahwen.query.UpdateScan
import dev.komu.ahwen.tx.Transaction

class IndexUpdatePlanner(private val metadataManager: MetadataManager) : UpdatePlanner {

    override fun executeInsert(data: InsertData, tx: Transaction): Int {
        val plan = TablePlan(data.table, metadataManager, tx)

        val scan = plan.open() as UpdateScan
        scan.insert()
        val rid = scan.rid

        val indices = metadataManager.getIndexInfo(data.table, tx)

        for ((field, value) in data.fields.zip(data.values)) {
            scan.setVal(field, value)

            val indexInfo = indices[field]
            if (indexInfo != null) {
                val index = indexInfo.open()
                index.insert(value, rid)
                index.close()
            }
        }
        scan.close()
        return 1
    }

    override fun executeDelete(data: DeleteData, tx: Transaction): Int {
        val tablePlan = TablePlan(data.table, metadataManager, tx)
        val select = SelectPlan(tablePlan, data.predicate)
        val scan = select.open() as UpdateScan
        val indices = metadataManager.getIndexInfo(data.table, tx)

        var count = 0
        while (scan.next()) {
            val rid = scan.rid

            for ((fieldName, indexInfo) in indices) {
                val value = scan.getVal(fieldName)
                val index = indexInfo.open()
                index.delete(value, rid)
                index.close()
            }
            scan.delete()
            count++
        }
        scan.close()
        return count
    }

    override fun executeModify(data: ModifyData, tx: Transaction): Int {
        val table = TablePlan(data.table, metadataManager, tx)
        val select = SelectPlan(table, data.predicate)
        val indexInfo = metadataManager.getIndexInfo(data.table, tx)[data.fieldName]
        val index = indexInfo?.open()

        val scan = select.open() as UpdateScan
        var count = 0
        while (scan.next()) {
            val newValue = data.newValue.evaluate(scan)
            val oldValue = scan.getVal(data.fieldName)
            scan.setVal(data.fieldName, newValue)

            if (index != null) {
                val rid = scan.rid
                index.delete(oldValue, rid)
                index.insert(newValue, rid)
            }
            count++
        }

        index?.close()
        scan.close()
        return count
    }

    override fun executeCreateTable(data: CreateTableData, tx: Transaction): Int {
        metadataManager.createTable(data.table, data.schema, tx)
        return 0
    }

    override fun executeCreateView(data: CreateViewData, tx: Transaction): Int {
        metadataManager.createView(data.view, data.viewDefinition, tx)
        return 0
    }

    override fun executeCreateIndex(data: CreateIndexData, tx: Transaction): Int {
        metadataManager.createIndex(data.index, data.table, data.field, tx)
        return 0
    }

}
