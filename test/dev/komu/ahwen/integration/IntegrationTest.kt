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
            stmt.executeUpdate("create index personIdIx on person (id)")
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

    @Test
    fun joins(@TempDir dir: File) {
        val connection = AhwenConnection(File(dir, "db"))

        val departmentsByEmployee = mutableMapOf<String, String>()
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("create table department (deptid int, deptname varchar(30))")
            stmt.executeUpdate("create table employee (empid int, empname varchar(30), empdeptid int)")
            stmt.executeUpdate("create index deptPkIx on department (deptid)")
            stmt.executeUpdate("insert into department (deptid, deptname) values (1, 'HR')")
            stmt.executeUpdate("insert into department (deptid, deptname) values (2, 'R&D')")
            stmt.executeUpdate("insert into employee (empid, empname, empdeptid) values (1, 'Bob', 1)")
            stmt.executeUpdate("insert into employee (empid, empname, empdeptid) values (2, 'Mike', 2)")
            stmt.executeUpdate("insert into employee (empid, empname, empdeptid) values (3, 'Cooper', 2)")

            stmt.executeQuery("select empname, deptname from employee, department where deptid = empdeptid").use { rs ->
                while (rs.next())
                    departmentsByEmployee[rs.getString("empname")] = rs.getString("deptname")
            }
        }

        assertEquals(mapOf("Bob" to "HR", "Mike" to "R&D", "Cooper" to "R&D"), departmentsByEmployee)
    }

    @Test
    fun `order by`(@TempDir dir: File) {
        val connection = AhwenConnection(File(dir, "db"))

        val numbers = mutableListOf<Int>()
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("create table numbers (value int, name varchar(30))")
            stmt.executeUpdate("insert into numbers (value, name) values (1, 'one')")
            stmt.executeUpdate("insert into numbers (value, name) values (2, 'two')")
            stmt.executeUpdate("insert into numbers (value, name) values (3, 'three')")
            stmt.executeUpdate("insert into numbers (value, name) values (4, 'four')")

            stmt.executeQuery("select value from numbers order by name").use { rs ->
                while (rs.next())
                    numbers += rs.getInt("value")
            }
        }

        assertEquals(listOf(4, 1, 3, 2), numbers)
    }

    @Test
    fun `group by`(@TempDir dir: File) {
        val connection = AhwenConnection(File(dir, "db"))

        val result = mutableMapOf<String, Pair<Int, Int>>()
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("create table foo (key varchar(10), value int)")
            stmt.executeUpdate("insert into foo (key, value) values ('a', 10)")
            stmt.executeUpdate("insert into foo (key, value) values ('a', 20)")
            stmt.executeUpdate("insert into foo (key, value) values ('b', 30)")
            stmt.executeUpdate("insert into foo (key, value) values ('c', 40)")
            stmt.executeUpdate("insert into foo (key, value) values ('c', 60)")
            stmt.executeUpdate("insert into foo (key, value) values ('c', 50)")

            stmt.executeQuery("select key, count(value), max(value) from foo group by key").use { rs ->
                while (rs.next())
                    result[rs.getString("key")] = Pair(rs.getInt("countofvalue"), rs.getInt("maxofvalue"))
            }
        }

        assertEquals(mapOf(
            "a" to Pair(2, 20),
            "b" to Pair(1, 30),
            "c" to Pair(3, 60)
        ), result)
    }
}
