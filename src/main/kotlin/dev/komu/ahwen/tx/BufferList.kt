package dev.komu.ahwen.tx

import dev.komu.ahwen.buffer.Buffer
import dev.komu.ahwen.buffer.BufferManager
import dev.komu.ahwen.buffer.PageFormatter
import dev.komu.ahwen.file.Block

class BufferList(private val bufferManager: BufferManager) {

    private val buffers = mutableMapOf<Block, Buffer>()
    private val pins = arrayListOf<Block>()

    fun getBuffer(block: Block): Buffer =
        buffers[block] ?: error("no buffer for block $block")

    fun pin(block: Block) {
        val buffer = bufferManager.pin(block)
        buffers[block] = buffer
        pins += block
    }

    fun pinNew(fileName: String, formatter: PageFormatter): Block {
        val buffer = bufferManager.pinNew(fileName, formatter)
        val block = buffer.block ?: error("no block for new buffer")
        buffers[block] = buffer
        pins += block
        return block
    }

    fun unpin(block: Block) {
        val buffer = getBuffer(block)
        bufferManager.unpin(buffer)
        pins -= block
        if (block !in pins)
            buffers.remove(block)
    }

    fun unpinAll() {
        for (block in pins) {
            val buffer = getBuffer(block)
            bufferManager.unpin(buffer)
        }
        buffers.clear()
        pins.clear()
    }
}
