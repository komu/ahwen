package dev.komu.ahwen.buffer

import dev.komu.ahwen.file.Page

/**
 * Strategy for initializing a newly allocated [Page].
 */
interface PageFormatter {
    fun format(page: Page)
}
