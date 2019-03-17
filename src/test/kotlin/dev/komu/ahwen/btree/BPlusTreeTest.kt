package dev.komu.ahwen.btree

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.*

internal class BPlusTreeTest {

    @Test
    fun `insert and retrieve values`() {
        val tree = BPlusTree<Int, String>(4)
        val random = Random(0)

        for (i in (0..99).toList().shuffled(random))
            tree.insert(i, "v$i")

        for (i in 0..99)
            assertEquals("v$i", tree[i], "get k$i")
    }

    @Test
    fun `remove values`() {
        val tree = BPlusTree<Int, String>(4)
        val random = Random(0)

        for (i in (0..99).toList().shuffled(random))
            tree.insert(i, "v$i")

        for (i in (0..49).toList().shuffled(random))
            tree.remove(i)

        for (i in 0..49)
            assertNull(tree[i])

        for (i in 50..99)
            assertEquals("v$i", tree[i])
    }

    @Test
    fun `clean everything`() {
        val tree = BPlusTree<Int, String>(4)

        val random = Random(0)

        for (i in (0..99).toList().shuffled(random))
            tree.insert(i, "v$i")

        for (i in (0..99).toList().shuffled(random))
            tree.remove(i)
    }

    @Test
    fun `random mutations`() {
        val tree = BPlusTree<Int, Int>(8)

        val random = Random(0)
        repeat(10_000) {
            val num = random.nextInt(50)
            if (tree[num] == null) {
                tree.insert(num, 1)
            } else {
                tree.remove(num)
            }
        }
    }

    @Test
    fun entries() {
        val random = Random(2)
        val tree = BPlusTree<Int, String>(8)

        val size = 1000
        val allValues = (0..size).toList()
        for (i in allValues.shuffled(random))
            tree.insert(i, "v$i")

        val removed = allValues.shuffled(random).take(size / 2)
        for (i in removed)
            tree.remove(i)

        for (i in removed)
            assertNull(tree[i], "value for $i")

        tree.checkInvariants()

        val remaining = allValues - removed

        for (i in remaining)
            assertEquals("v$i", tree[i], "value for $i")

        assertEquals(remaining.size, tree.countEntries())
        assertEquals(remaining.map { it to "v$it" }, tree.entries().asSequence().toList())
    }
}
