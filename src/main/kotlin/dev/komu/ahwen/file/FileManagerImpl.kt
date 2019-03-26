package dev.komu.ahwen.file

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

internal class FileManagerImpl(private val dbDirectory: File) : FileManager {

    val isNew = !dbDirectory.exists()
    private val openFiles = mutableMapOf<String, FileChannel>()

    init {
        if (!dbDirectory.exists() && !dbDirectory.mkdirs())
            error("failed to create directory $dbDirectory")

        for (file in dbDirectory.listFiles())
            if (file.name.startsWith("temp"))
                file.delete()
    }

    @Synchronized
    override fun read(block: Block, bb: ByteBuffer) {
        bb.clear()
        val fc = getFile(block.filename)
        fc.read(bb, block.number.toLong() * bb.capacity())
    }

    @Synchronized
    override fun write(block: Block, bb: ByteBuffer) {
        bb.rewind()
        val fc = getFile(block.filename)
        fc.write(bb, block.number.toLong() * bb.capacity())
    }

    @Synchronized
    override fun append(fileName: String, bb: ByteBuffer): Block {
        val newBlockNum = size(fileName)
        val block = Block(fileName, newBlockNum)
        write(block, bb)
        return block
    }

    @Synchronized
    override fun size(fileName: String): Int {
        val fc = getFile(fileName)
        return fc.size().toInt() / Page.BLOCK_SIZE
    }

    private fun getFile(fileName: String): FileChannel =
        openFiles.getOrPut(fileName) {
            val dbTable = File(dbDirectory, fileName)
            val f = RandomAccessFile(dbTable, "rws")
            f.channel
        }
}
