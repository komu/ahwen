package dev.komu.ahwen.file

import java.nio.ByteBuffer

interface FileManager {
    fun read(block: Block, contents: ByteBuffer)
    fun write(block: Block, contents: ByteBuffer)
    fun append(fileName: String, contents: ByteBuffer): Block
    fun size(fileName: String): Int
}

