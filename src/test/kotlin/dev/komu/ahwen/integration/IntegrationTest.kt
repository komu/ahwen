package dev.komu.ahwen.integration

import dev.komu.ahwen.jdbc.AhwenConnection
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals

class IntegrationTest {

    @Test
    fun `create table, insert and retrieve`(@TempDir dir: File) {
        val connection = AhwenConnection(File(dir, "db"))

        val results = mutableListOf<Int>()
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("create table person (id int, name varchar(30))")
            stmt.executeUpdate("insert into person (id, name) values (1, 'Fred')")
            stmt.executeUpdate("insert into person (id, name) values (2, 'Bob')")
            stmt.executeUpdate("insert into person (id, name) values (3, 'Hank')")
            stmt.executeUpdate("insert into person (id, name) values (4, 'Hank')")

            stmt.executeQuery("select id from person where name = 'Hank'").use { rs ->
                while (rs.next()) {
                    results += rs.getInt("id")
                }
            }
        }

        assertEquals(listOf(3, 4), results)
    }
}
