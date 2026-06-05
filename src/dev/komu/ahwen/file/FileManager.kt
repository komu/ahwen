package dev.komu.ahwen.file

import dev.komu.ahwen.types.FileName
import java.nio.ByteBuffer

/**
 * Low level file management.
 *
 * [FileManager] supports addressing data by [Block]s, assumes that the size of the blocks
 * is [Page.BLOCK_SIZE]. E.g. if block number is `3` and block size is `4096`, then bytes
 * are read from offset `3 * 4096` of the target file.
 *
 * Write operations are guaranteed to be flushed to disk before calls return.
 */
interface FileManager {

    /**
     * Reads a buffer of bytes from given block, which must exist in the file.
     */
    fun read(block: Block, bb: ByteBuffer)

    /**
     * Writes a buffer to of bytes to given block, which must exist in the file.
     */
    fun write(block: Block, bb: ByteBuffer)

    /**
     * Adds a new block to given file and returns identifier for it.
     * If the file does not exist, it will be created.
     */
    fun append(fileName: FileName, bb: ByteBuffer): Block

    /**
     * Returns the amount of blocks stored in the file. If the file does not exist,
     * an empty file will be created.
     */
    fun size(fileName: FileName): Int

    /**
     * Returns the number of last block, or `null` if the file is empty. If the file
     * does not exist, an empty file will be created.
     */
    fun lastBlock(fileName: FileName): Int? =
        size(fileName).takeIf { it > 0 }?.let { it - 1 }
}
