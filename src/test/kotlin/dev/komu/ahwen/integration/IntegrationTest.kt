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
//            stmt.executeUpdate("create index employeeDeptIx on employee (empdeptid)")
            stmt.executeUpdate("insert into department (deptid, deptname) values (1, 'H&R')")
            stmt.executeUpdate("insert into department (deptid, deptname) values (2, 'R&D')")
            stmt.executeUpdate("insert into employee (empid, empname, empdeptid) values (1, 'Bob', 1)")
            stmt.executeUpdate("insert into employee (empid, empname, empdeptid) values (2, 'Mike', 2)")
            stmt.executeUpdate("insert into employee (empid, empname, empdeptid) values (3, 'Cooper', 2)")

            stmt.executeQuery("select empname, deptname from employee, department where deptid = empdeptid").use { rs ->
                while (rs.next())
                    departmentsByEmployee[rs.getString("empname")] = rs.getString("deptname")
            }
        }

        assertEquals(mapOf("Bob" to "H&R", "Mike" to "R&D", "Cooper" to "R&D"), departmentsByEmployee)
    }
}
