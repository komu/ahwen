package dev.komu.ahwen.log

import dev.komu.ahwen.file.Page
import dev.komu.ahwen.file.Page.Companion.strSize

/**
 * Provides unstructured low-level read access to log records.
 *
 * Higher level log parsing can be implemented on top of this.
 */
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
