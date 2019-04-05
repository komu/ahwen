package dev.komu.ahwen.file

import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.ByteBuffer

internal class DefaultFileManagerTest {

    private val buffer = ByteBuffer.allocateDirect(BLOCK_SIZE)

    @Test
    fun `appending blocks`(@TempDir dir: File) {
        val fm = DefaultFileManager(dir)

        assertEquals(0, fm.size("my-file"))

        fm.append("my-file", buffer)
        assertEquals(1, fm.size("my-file"))

        fm.append("my-file", buffer)
        assertEquals(2, fm.size("my-file"))

        fm.append("my-file2", buffer)
        assertEquals(2, fm.size("my-file"))
        assertEquals(1, fm.size("my-file2"))
    }

    @Test
    fun `reading and writing blocks`(@TempDir dir: File) {
        val fm = DefaultFileManager(dir)

        buffer.put(byteArrayOf(1, 2, 3))
        val block1 = fm.append("my-file", buffer)

        buffer.flip()
        buffer.put(byteArrayOf(6, 5, 4))
        val block2 = fm.append("my-file", buffer)

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
