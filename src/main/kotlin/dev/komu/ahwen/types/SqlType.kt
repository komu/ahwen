package dev.komu.ahwen.types

import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.IntConstant
import dev.komu.ahwen.query.StringConstant
import java.sql.Types

enum class SqlType(val code: Int) {
    INTEGER(Types.INTEGER),
    VARCHAR(Types.VARCHAR);

    val minimumValue: Constant
        get() = when (this) {
            INTEGER -> IntConstant(Int.MIN_VALUE)
            VARCHAR -> StringConstant("")
        }

    companion object {

        operator fun invoke(code: Int) =
            values().find { it.code == code } ?: error("invalid type-code: $code")
    }
}
