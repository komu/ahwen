package dev.komu.ahwen.planner

import dev.komu.ahwen.metadata.MetadataManager
import dev.komu.ahwen.parse.*
import dev.komu.ahwen.query.SelectPlan
import dev.komu.ahwen.query.TablePlan
import dev.komu.ahwen.query.UpdateScan
import dev.komu.ahwen.tx.Transaction

class BasicUpdatePlanner(private val metadataManager: MetadataManager) : UpdatePlanner {

    override fun executeDelete(data: DeleteData, tx: Transaction): Int {
        val tablePlan = TablePlan(data.table, metadataManager, tx)
        val select = SelectPlan(tablePlan, data.predicate)
        val scan = select.open() as UpdateScan
        var count = 0
        while (scan.next()) {
            scan.delete()
            count++
        }
        scan.close()
        return count
    }

    override fun executeModify(data: ModifyData, tx: Transaction): Int {
        val table = TablePlan(data.table, metadataManager, tx)
        val select = SelectPlan(table, data.predicate)
        val scan = select.open() as UpdateScan
        var count = 0
        while (scan.next()) {
            val constant = data.newValue.evaluate(scan)
            scan.setVal(data.fieldName, constant)
            count++
        }
        scan.close()
        return count
    }

    override fun executeInsert(data: InsertData, tx: Transaction): Int {
        val plan = TablePlan(data.table, metadataManager, tx)
        val scan = plan.open() as UpdateScan
        scan.insert()
        for ((field, value) in data.fields.zip(data.values)) {
            scan.setVal(field, value)
        }
        scan.close()
        return 1
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
