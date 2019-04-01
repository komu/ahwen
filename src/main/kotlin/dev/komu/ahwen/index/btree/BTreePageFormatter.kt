package dev.komu.ahwen.index.btree

import dev.komu.ahwen.buffer.PageFormatter
import dev.komu.ahwen.file.Page
import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.record.TableInfo
import java.sql.Types

class BTreePageFormatter(
    private val ti: TableInfo,
    private val flag: Int
) :
    PageFormatter {

    override fun format(page: Page) {
        page.setInt(0, flag)
        page.setInt(Int.SIZE_BYTES, 0)
        val recordSize = ti.recordLength

        var pos = 2 * Int.SIZE_BYTES
        while (pos + recordSize <= BLOCK_SIZE) {
            makeDefaultRecord(page, pos)
            pos += recordSize
        }
    }

    private fun makeDefaultRecord(page: Page, pos: Int) {
        for (fieldName in ti.schema.fields) {
            val offset = ti.offset(fieldName)
            val type = ti.schema.type(fieldName)
            when (type) {
                Types.INTEGER -> page.setInt(pos + offset, 0)
                Types.VARCHAR -> page.setString(pos + offset, "")
                else -> error("invalid type $type")
            }
        }
    }
}
