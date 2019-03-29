package dev.komu.ahwen.jdbc

import dev.komu.ahwen.utils.unimplemented
import java.io.File
import java.sql.Connection
import java.sql.Statement

class AhwenConnection(dir: File) : Connection by unimplemented() {

    private val db = AhwenDatabase(dir)

    override fun createStatement(): Statement =
        AhwenStatement(db)

    override fun getAutoCommit(): Boolean =
        true

    override fun setAutoCommit(autoCommit: Boolean) {
        error("currently only auto-commit is supported")
    }
}
