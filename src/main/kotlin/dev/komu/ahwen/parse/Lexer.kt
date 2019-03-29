package dev.komu.ahwen.parse

import java.io.IOException
import java.io.StreamTokenizer
import java.io.StringReader

class Lexer(s: String) {

    private val tok = StreamTokenizer(StringReader(s)).apply {
        ordinaryChar('.'.toInt())
        lowerCaseMode(true)
        nextToken()
    }

    // methods to check status of the current token
    fun matchDelim(d: Char) =
        d == tok.ttype.toChar()

    fun matchIntConstant() =
        tok.ttype == StreamTokenizer.TT_NUMBER

    fun matchStringConstant() =
        tok.ttype.toChar() == '\''

    fun matchKeyword(w: String) =
        tok.ttype == StreamTokenizer.TT_WORD && tok.sval == w

    fun matchId() =
        tok.ttype == StreamTokenizer.TT_WORD && tok.sval !in keywords

    // methods to eat the current token
    fun eatDelim(d: Char) {
        if (!matchDelim(d))
            throw BadSyntaxException()
        nextToken()
    }

    fun eatIntConstant(): Int {
        if (!matchIntConstant())
            throw BadSyntaxException()
        val value = tok.nval.toInt()
        nextToken()
        return value
    }

    fun eatStringConstant(): String {
        if (!matchStringConstant())
            throw BadSyntaxException()
        val value = tok.sval
        nextToken()
        return value
    }

    fun eatKeyword(w: String) {
        if (!matchKeyword(w))
            throw BadSyntaxException()
        nextToken()
    }

    fun eatId(): String {
        if (!matchId())
            throw BadSyntaxException()
        val value = tok.sval
        nextToken()
        return value
    }

    private fun nextToken() {
        try {
            tok.nextToken()
        } catch (e: IOException) {
            throw BadSyntaxException()
        }
    }

    companion object {

        private val keywords = listOf(
            "select", "from", "where", "and", "insert", "into", "values", "int", "varchar",
            "update", "set", "delete", "on", "create", "table", "view", "as", "index"
        )
    }
}
