package dev.komu.ahwen.file

/**
 * Uniquely identifies a block in some file.
 *
 * Blocks can be loaded to [Page]s for accessing their data.
 */
data class Block(val filename: String, val number: Int)
