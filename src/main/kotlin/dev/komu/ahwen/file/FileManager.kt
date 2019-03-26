package dev.komu.ahwen.file

import java.nio.ByteBuffer

interface FileManager {
    fun read(block: Block, bb: ByteBuffer)
    fun write(block: Block, bb: ByteBuffer)
    fun append(fileName: String, bb: ByteBuffer): Block
    fun size(fileName: String): Int
}

