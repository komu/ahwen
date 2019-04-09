package dev.komu.ahwen.record

import dev.komu.ahwen.buffer.PageFormatter
import dev.komu.ahwen.file.Page
import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE

/**
 * [PageFormatter] that initializes the page with empty records of type [ti].
 */
class RecordFormatter(private val ti: TableInfo) : PageFormatter {

    override fun format(page: Page) {
        val recSize = ti.recordLength + Int.SIZE_BYTES

        var pos = 0
        while (pos + recSize <= BLOCK_SIZE) {
            page.setInt(pos, RecordPage.EMPTY)
            makeDefaultRecord(page, pos)
            pos += recSize
        }
    }

    private fun makeDefaultRecord(page: Page, pos: Int) {
        for (column in ti.schema.columns) {
            val offset = ti.offset(column)
            val position = pos + Int.SIZE_BYTES + offset
            val type = ti.schema.type(column)
            page[position] = type.defaultValue
        }
    }
}
