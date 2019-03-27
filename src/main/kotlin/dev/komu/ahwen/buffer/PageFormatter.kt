package dev.komu.ahwen.buffer

import dev.komu.ahwen.file.Page

interface PageFormatter {
    fun format(page: Page)
}
