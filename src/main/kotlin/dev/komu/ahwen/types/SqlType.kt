package dev.komu.ahwen.types

import java.sql.Types

enum class SqlType(val code: Int) {
    INTEGER(Types.INTEGER),
    VARCHAR(Types.VARCHAR);

    companion object {

        operator fun invoke(code: Int) =
            values().find { it.code == code } ?: error("invalid type-code: $code")
    }
}
