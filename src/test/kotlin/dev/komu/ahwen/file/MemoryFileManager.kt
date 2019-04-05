package dev.komu.ahwen.file

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A simple in-memory implementation of [FileManager] useful for unit tests.
 */
class MemoryFileManager : FileManager {

    private val blocksByFiles = mutableMapOf<String, Int>()
    private val dataByBlock = mutableMapOf<Block, ByteArray>()
    private val lock = ReentrantLock()

    override fun read(block: Block, bb: ByteBuffer) {
        lock.withLock {
            bb.position(0)
            bb.put(getBlockData(block))
        }
    }

    override fun write(block: Block, bb: ByteBuffer) {
        lock.withLock {
            bb.position(0)
            bb.get(getBlockData(block))
        }
    }

    override fun append(fileName: String, bb: ByteBuffer): Block {
        lock.withLock {
            val size = size(fileName)
            blocksByFiles[fileName] = size + 1
            val block = Block(fileName, size)
            write(block, bb)
            return block
        }
    }

    override fun size(fileName: String): Int =
        lock.withLock {
            blocksByFiles.getOrPut(fileName) { 0 }
        }

    private fun getBlockData(block: Block): ByteArray =
        dataByBlock.getOrPut(block) { ByteArray(Page.BLOCK_SIZE) }
}
