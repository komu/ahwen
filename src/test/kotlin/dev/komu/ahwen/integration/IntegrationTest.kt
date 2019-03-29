package dev.komu.ahwen.integration

import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.file.FileManagerImpl
import dev.komu.ahwen.log.LogManager
import dev.komu.ahwen.metadata.MetadataManager
import dev.komu.ahwen.planner.Planner
import dev.komu.ahwen.tx.Transaction
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals

class IntegrationTest {

    @Test
    fun `create table, insert and retrieve`(@TempDir dir: File) {
        val fileManager = FileManagerImpl(File(dir, "db"))
        val logManager = LogManager(fileManager, "log")
        val bufferManager = BufferManager(1000, fileManager, logManager)

        val tx1 = Transaction(logManager, bufferManager, fileManager)
        val metadataManager = MetadataManager(fileManager.isNew, tx1)
        tx1.commit()

        val planner = Planner(metadataManager)

        Transaction(logManager, bufferManager, fileManager).also { tx ->
            planner.executeUpdate("create table person (id int, name varchar(30))", tx)
            planner.executeUpdate("insert into person (id, name) values (1, 'Fred')", tx)
            planner.executeUpdate("insert into person (id, name) values (2, 'Bob')", tx)
            planner.executeUpdate("insert into person (id, name) values (3, 'Hank')", tx)
            planner.executeUpdate("insert into person (id, name) values (4, 'Hank')", tx)
            tx.commit()
        }

        val results = mutableListOf<Int>()
        Transaction(logManager, bufferManager, fileManager).also { tx ->
            val plan = planner.createQueryPlan("select id from person where name = 'Hank'", tx)
            val scan = plan.open()

            while (scan.next()) {
                results += scan.getInt("id")
            }
            scan.close()
            tx.commit()
        }

        assertEquals(listOf(3, 4), results)
    }
}
