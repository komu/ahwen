package dev.komu.ahwen.file

import java.nio.ByteBuffer

class Page(private val fileManager: FileManager) {

    private val contents: ByteBuffer = ByteBuffer.allocateDirect(BLOCK_SIZE)

    @Synchronized
    fun read(block: Block) {
        fileManager.read(block, contents)
    }

    @Synchronized
    fun write(block: Block) {
        fileManager.write(block, contents)
    }

    @Synchronized
    fun append(fileName: String): Block {
        return fileManager.append(fileName, contents)
    }

    @Synchronized
    fun getInt(offset: Int): Int {
        contents.position(offset)
        return contents.int
    }

    @Synchronized
    fun setInt(offset: Int, value: Int) {
        contents.position(offset)
        contents.putInt(value)
    }

    @Synchronized
    fun getString(offset: Int): String {
        contents.position(offset)
        val len = contents.int
        val bytes = ByteArray(len)
        contents.get(bytes)
        return String(bytes, charset)
    }

    @Synchronized
    fun setString(offset: Int, value: String) {
        contents.position(offset)
        val bytes = value.toByteArray(charset)
        contents.putInt(bytes.size)
        contents.put(bytes)
    }

    companion object {

        const val BLOCK_SIZE = 4096
        private val charset = Charsets.UTF_8
        private val bytesPerChar = charset.newEncoder().maxBytesPerChar().toInt()

        fun strSize(len: Int): Int =
            Int.SIZE_BYTES + (len * bytesPerChar)
    }
}
