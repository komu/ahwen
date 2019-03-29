package dev.komu.ahwen.parse

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class ParserTest {

    @Test
    fun `parsing select`() {
        val parser = Parser(Lexer("select x from Foo where x = 42"))

        assertEquals("select x from foo where x=42", parser.query().toString())
    }
}
