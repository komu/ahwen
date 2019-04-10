package dev.komu.ahwen.jdbc

import dev.komu.ahwen.query.SqlValue
import dev.komu.ahwen.query.forEach
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.utils.unimplemented
import java.sql.ResultSet
import java.sql.Statement

class AhwenStatement(private val db: AhwenDatabase) : Statement by unimplemented() {

    override fun executeUpdate(sql: String): Int = withTransaction { tx ->
        db.planner.executeUpdate(sql, tx)
    }

    override fun executeQuery(sql: String): ResultSet = withTransaction { tx ->
        val plan = db.planner.createQueryPlan(sql, tx)
        plan.open().use { scan ->
            val rows = mutableListOf<Map<String, SqlValue>>()
            scan.forEach {
                rows += plan.schema.columns.map { it.value to scan[it] }.toMap()
            }
            return AhwenResultSet(rows)
        }
    }

    private inline fun <T> withTransaction(callback: (Transaction) -> T): T {
        val tx = db.beginTransaction()
        try {
            val result = callback(tx)
            tx.commit()
            return result
        } catch (e: Throwable) {
            tx.rollback()
            throw e
        }
    }

    override fun close() {
    }
}
