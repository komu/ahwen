package dev.komu.ahwen.parse

import java.io.IOException
import java.io.StreamTokenizer
import java.io.StringReader

class Lexer(s: String) {

    private val tok = StreamTokenizer(StringReader(s)).apply {
        ordinaryChar('.'.code)
        lowerCaseMode(true)
        nextToken()
    }

    // methods to check status of the current token
    fun matchDelim(d: Char) =
        d == tok.ttype.toChar()

    private fun matchIntConstant() =
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
            throw BadSyntaxException("expected $d")
        nextToken()
    }

    fun eatIntConstant(): Int {
        if (!matchIntConstant())
            throw BadSyntaxException("expected int constant")
        val value = tok.nval.toInt()
        nextToken()
        return value
    }

    fun eatStringConstant(): String {
        if (!matchStringConstant())
            throw BadSyntaxException("expected string constant")
        val value = tok.sval
        nextToken()
        return value
    }

    fun eatKeyword(w: String) {
        if (!matchKeyword(w))
            throw BadSyntaxException("expected keyword $w")
        nextToken()
    }

    fun eatId(): String {
        if (!matchId())
            throw BadSyntaxException("expected identifier")
        val value = tok.sval
        nextToken()
        return value
    }

    private fun nextToken() {
        try {
            tok.nextToken()
        } catch (_: IOException) {
            throw BadSyntaxException("unexpected eof")
        }
    }

    fun assertEnd() {
        tok.nextToken()
        if (tok.ttype != StreamTokenizer.TT_EOF)
            throw BadSyntaxException("expected eof")
    }

    companion object {

        private val keywords = listOf(
            "select", "from", "where", "and", "insert", "into", "values", "int", "varchar",
            "update", "set", "delete", "on", "create", "table", "view", "as", "index", "order",
            "by", "group"
        )
    }
}
