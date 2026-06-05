package dev.komu.ahwen.query

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class BufferNeedsTest {

    @Test
    fun `best factors`() {
        assertEquals(1, bestFactor(60, 0))
        assertEquals(1, bestFactor(60, 1))
        assertEquals(2, bestFactor(60, 2))
        assertEquals(3, bestFactor(60, 3))
        assertEquals(4, bestFactor(60, 4))
        assertEquals(5, bestFactor(60, 5))
        assertEquals(6, bestFactor(60, 6))
        assertEquals(7, bestFactor(60, 7))
        assertEquals(8, bestFactor(60, 8))
        assertEquals(8, bestFactor(60, 9))
        assertEquals(10, bestFactor(60, 10))
        assertEquals(10, bestFactor(60, 11))
        assertEquals(12, bestFactor(60, 12))
        assertEquals(12, bestFactor(60, 13))
        assertEquals(12, bestFactor(60, 14))
        assertEquals(15, bestFactor(60, 15))
        assertEquals(15, bestFactor(60, 16))
        assertEquals(15, bestFactor(60, 17))
        assertEquals(15, bestFactor(60, 18))
        assertEquals(15, bestFactor(60, 19))
        assertEquals(20, bestFactor(60, 20))
        assertEquals(20, bestFactor(60, 29))
        assertEquals(30, bestFactor(60, 30))
        assertEquals(30, bestFactor(60, 50))
        assertEquals(30, bestFactor(60, 59))
        assertEquals(60, bestFactor(60, 60))
        assertEquals(60, bestFactor(60, 61))
        assertEquals(60, bestFactor(60, 1000))
    }
}
