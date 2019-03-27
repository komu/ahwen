package dev.komu.ahwen.record

import dev.komu.ahwen.buffer.PageFormatter
import dev.komu.ahwen.file.Page
import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import java.sql.Types

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
        for (field in ti.schema.fields) {
            val offset = ti.offset(field)
            val type = ti.schema.type(field)
            val position = pos + Int.SIZE_BYTES + offset
            when (type) {
                Types.INTEGER ->
                    page.setInt(position, 0)
                Types.VARCHAR ->
                    page.setString(position, "")
                else ->
                    error("unexpected type $type")
            }
        }
    }
}
