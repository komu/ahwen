package dev.komu.ahwen.file

import dev.komu.ahwen.types.FileName
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Default, file system backed, implementation of [FileManager].
 *
 * The database is stored in a set of files residing in [dbDirectory]. This class
 * is responsible for block-level access to those files.
 */
class DefaultFileManager(private val dbDirectory: File) : FileManager {

    val isNew = !dbDirectory.exists()
    private val openFiles = mutableMapOf<FileName, FileChannel>()
    private val lock = ReentrantLock()
    val stats = FileStats()

    init {
        if (!dbDirectory.exists() && !dbDirectory.mkdirs())
            throw IOException("failed to create directory $dbDirectory")

        // Clear the temporary files from previous run
        for (file in dbDirectory.listFiles())
            if (file.name.startsWith("temp"))
                file.delete()
    }

    override fun read(block: Block, bb: ByteBuffer) {
        stats.incrementReads()
        lock.withLock {
            bb.clear()
            val fc = getFile(block.filename)
            fc.read(bb, block.number.toLong() * bb.capacity())
        }
    }

    override fun write(block: Block, bb: ByteBuffer) {
        stats.incrementWrites()
        lock.withLock {
            bb.rewind()
            val fc = getFile(block.filename)
            fc.write(bb, block.number.toLong() * bb.capacity())
        }
    }

    override fun append(fileName: FileName, bb: ByteBuffer): Block {
        lock.withLock {
            val newBlockNum = size(fileName)
            val block = Block(fileName, newBlockNum)
            write(block, bb)
            return block
        }
    }

    override fun size(fileName: FileName): Int {
        lock.withLock {
            val fc = getFile(fileName)
            return fc.size().toInt() / Page.BLOCK_SIZE
        }
    }

    private fun getFile(fileName: FileName): FileChannel =
        openFiles.getOrPut(fileName) {
            val dbTable = File(dbDirectory, fileName.value)
            RandomAccessFile(dbTable, "rws").channel
        }
}

