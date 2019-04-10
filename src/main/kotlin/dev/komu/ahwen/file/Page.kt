@file:Suppress("UsePropertyAccessSyntax")

package dev.komu.ahwen.file

import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.query.Constant
import dev.komu.ahwen.query.IntConstant
import dev.komu.ahwen.query.StringConstant
import dev.komu.ahwen.types.FileName
import dev.komu.ahwen.types.SqlType
import dev.komu.ahwen.types.SqlType.*
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

    operator fun set(offset: Int, value: Constant) {
        lock.withLock {
            contents.position(offset)
            when (value) {
                is IntConstant ->
                    contents.putInt(value.value)
                is StringConstant ->
                    contents.writeString(value.value)
            }
        }
    }

    fun getValue(offset: Int, type: SqlType): Constant =
        lock.withLock {
            contents.position(offset)
            when (type) {
                INTEGER -> IntConstant(contents.getInt())
                VARCHAR -> StringConstant(contents.readString())
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

        private fun ByteBuffer.readString(): String {
            val len = getInt()
            val bytes = ByteArray(len)

            get(bytes)
            return String(bytes, charset)
        }

        private fun ByteBuffer.writeString(value: String) {
            val bytes = value.toByteArray(charset)

            putInt(bytes.size)
            put(bytes)
        }
    }
}

fun Page.getInt(offset: Int): Int =
    (getValue(offset, SqlType.INTEGER) as IntConstant).value

operator fun Page.set(offset: Int, value: Int) {
    this[offset] = IntConstant(value)
}

