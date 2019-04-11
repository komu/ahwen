package dev.komu.ahwen.utils

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class LRUSetTest {

    @Test
    fun `insert and touch entries in LRUSet`() {
        val lru = LRUSet(listOf("foo", "bar", "baz"))

        assertEquals(listOf("foo", "bar", "baz"), lru.toList())

        lru.touch("foo")
        assertEquals(listOf("bar", "baz", "foo"), lru.toList())

        lru.touch("baz")
        assertEquals(listOf("bar", "foo", "baz"), lru.toList())

        lru.touch("bar")
        assertEquals(listOf("foo", "baz", "bar"), lru.toList())

        lru.touch("baz")
        assertEquals(listOf("foo", "bar", "baz"), lru.toList())

        assertEquals("foo", lru.removeEldest())
        assertEquals(listOf("bar", "baz"), lru.toList())

        lru.touch("bar")
        assertEquals(listOf("baz", "bar"), lru.toList())
    }
}
