package dev.komu.ahwen.log

import dev.komu.ahwen.file.Page
import dev.komu.ahwen.file.Page.Companion.strSize

class BasicLogRecord(private val page: Page, private var pos: Int) {

    fun nextInt(): Int {
        val result = page.getInt(pos)
        pos += Int.SIZE_BYTES
        return result
    }

    fun nextString(): String {
        val result = page.getString(pos)
        pos += strSize(result.length)
        return result
    }
}
