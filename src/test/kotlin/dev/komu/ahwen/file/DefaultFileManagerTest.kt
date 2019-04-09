package dev.komu.ahwen.file

import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.types.FileName
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.ByteBuffer

internal class DefaultFileManagerTest {

    private val buffer = ByteBuffer.allocateDirect(BLOCK_SIZE)
    private val myFile1 = FileName("my-file")
    private val myFile2 = FileName("my-file2")

    @Test
    fun `appending blocks`(@TempDir dir: File) {
        val fm = DefaultFileManager(dir)

        assertEquals(0, fm.size(myFile1))

        fm.append(myFile1, buffer)
        assertEquals(1, fm.size(myFile1))

        fm.append(myFile1, buffer)
        assertEquals(2, fm.size(myFile1))


        fm.append(myFile2, buffer)
        assertEquals(2, fm.size(myFile1))
        assertEquals(1, fm.size(myFile2))
    }

    @Test
    fun `reading and writing blocks`(@TempDir dir: File) {
        val fm = DefaultFileManager(dir)

        buffer.put(byteArrayOf(1, 2, 3))
        val block1 = fm.append(myFile1, buffer)

        buffer.flip()
        buffer.put(byteArrayOf(6, 5, 4))
        val block2 = fm.append(myFile1, buffer)

        fm.read(block1, buffer)
        buffer.flip()
        assertEquals(1, buffer.get())
        assertEquals(2, buffer.get())
        assertEquals(3, buffer.get())

        fm.read(block2, buffer)
        buffer.flip()
        assertEquals(6, buffer.get())
        assertEquals(5, buffer.get())
        assertEquals(4, buffer.get())
    }
}
