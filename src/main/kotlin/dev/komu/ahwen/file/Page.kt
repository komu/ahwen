@file:Suppress("UsePropertyAccessSyntax")

package dev.komu.ahwen.file

import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.IntConstant
import dev.komu.ahwen.query.StringConstant
import dev.komu.ahwen.types.FileName
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A page is [BLOCK_SIZE] worth of raw data loaded to memory.
 *
 * Pages will get reused: a single page will typically contain data from multiple [Block]s
 * during its lifetime.
 */
class Page(private val fileManager: FileManager) {

    private val contents: ByteBuffer = ByteBuffer.allocateDirect(BLOCK_SIZE)
    private val lock = ReentrantLock()

    fun read(block: Block) {
        lock.withLock {
            fileManager.read(block, contents)
        }
    }

    fun write(block: Block) {
        lock.withLock {
            fileManager.write(block, contents)
        }
    }

    fun append(fileName: FileName): Block {
        lock.withLock {
            return fileManager.append(fileName, contents)
        }
    }

    fun getInt(offset: Int): Int {
        lock.withLock {
            contents.position(offset)
            return contents.getInt()
        }
    }

    operator fun set(offset: Int, value: Constant) {
        lock.withLock {
            when (value) {
                is IntConstant -> setInt(offset, value.value)
                is StringConstant -> setString(offset, value.value)
            }
        }
    }

    fun setInt(offset: Int, value: Int) {
        lock.withLock {
            contents.position(offset)
            contents.putInt(value)
        }
    }

    fun getString(offset: Int): String {
        lock.withLock {
            contents.position(offset)
            val len = contents.getInt()
            val bytes = ByteArray(len)

            contents.get(bytes)
            return String(bytes, charset)
        }
    }

    fun setString(offset: Int, value: String) {
        lock.withLock {
            val bytes = value.toByteArray(charset)

            contents.position(offset)
            contents.putInt(bytes.size)
            contents.put(bytes)
        }
    }

    companion object {

        /**
         * Size of blocks, ie. the size of individual I/O operations. For best performance
         * this should correspond to the device block size of the operating system (typically 4k),
         * but it might be interesting to make it lower for testing purposes.
         */
        const val BLOCK_SIZE = 4096

        const val INT_SIZE = Int.SIZE_BYTES

        /** Character set used to store strings */
        private val charset = Charsets.UTF_8

        private val bytesPerChar = charset.newEncoder().maxBytesPerChar().toInt()

        /**
         * Returns the number of bytes needed to store a string of given length.
         *
         * Strings are represented by storing their length (32 bit integer) followed
         * by their characters.
         */
        fun strSize(len: Int): Int =
            INT_SIZE + (len * bytesPerChar)
    }
}
