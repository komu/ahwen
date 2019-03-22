package dev.komu.ahwen.utils

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class CollectionExtensionsTest {

    @Test
    fun `check isStrictlyAscending`() {
        assertTrue(emptyList<Int>().isStrictlyAscending())

        assertTrue(listOf(1).isStrictlyAscending())
        assertTrue(listOf(1, 2).isStrictlyAscending())
        assertTrue(listOf(1, 2, 3, 5, 10).isStrictlyAscending())

        assertFalse(listOf(2, 1).isStrictlyAscending())
        assertFalse(listOf(1, 3, 1).isStrictlyAscending())
        assertFalse(listOf(1, 1).isStrictlyAscending())
    }

    @Test
    fun `check subListToEnd`() {
        assertEquals(emptyList<Int>(), mutableListOf<Int>().subListToEnd(0))
        assertEquals(listOf(4, 5), mutableListOf(1, 2, 3, 4, 5).subListToEnd(3))
    }

    @Test
    fun `subListToEnd returns modifiable view`() {
        val original = mutableListOf(1, 2, 3, 4, 5)
        original.subListToEnd(3).clear()

        assertEquals(listOf(1, 2, 3), original)
    }
}
