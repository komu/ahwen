package dev.komu.ahwen.btree

/**
 * Represents a node in [BPlusTree].
 */
sealed class Node<K, V>(val nodeId: NodeId) where K : Comparable<K> {

    val keys = mutableListOf<K>()
    var dirty = true

    class Internal<K, V>(nodeId: NodeId) : Node<K, V>(nodeId) where K : Comparable<K> {
        val children = mutableListOf<NodeId>()

        fun merge(right: Internal<K, V>, key: K) {
            keys += key
            keys += right.keys
            children += right.children
            dirty = true
        }

        override fun toString(): String = "internal $keys"

        fun insertChild(key: K, child: NodeId) {
            val loc = keys.binarySearch(key)
            if (loc >= 0) {
                // children[loc + 1] = child
                error("trying to insert duplicate child")
            } else {
                val index = -loc - 1
                keys.add(index, key)
                children.add(index + 1, child)
                dirty = true
            }
        }

        fun findChildIndex(key: K): Int {
            val index = keys.binarySearch(key)
            return if (index >= 0) index + 1 else -index - 1
        }
    }

    class Leaf<K, V>(nodeId: NodeId) : Node<K, V>(nodeId) where K : Comparable<K> {
        val values = mutableListOf<V>()
        var next: Leaf<K, V>? = null

        val entries: Sequence<Pair<K, V>>
            get() = sequence {
                for ((i, key) in keys.withIndex())
                    yield(key to values[i])
            }

        operator fun get(key: K): V? {
            val index = keys.binarySearch(key)
            if (index < 0)
                return null

            return values[index]
        }

        fun merge(right: Leaf<K, V>) {
            keys += right.keys
            values += right.values
            next = right.next
        }

        fun moveFrom(from: Leaf<K, V>, count: Int, reversed: Boolean) {
            if (!reversed) {
                val movedKeys = from.keys.subList(0, count)
                val movedValues = from.values.subList(0, count)

                keys += movedKeys
                values += movedValues

                movedKeys.clear()
                movedValues.clear()

            } else {
                val movedKeys = from.keys.subList(from.keys.size - count, from.keys.size)
                val movedValues = from.values.subList(from.keys.size - count, from.keys.size)

                keys.addAll(0, movedKeys)
                values.addAll(0, movedValues)

                movedKeys.clear()
                movedValues.clear()
            }
        }

        override fun toString(): String = "leaf $keys"
    }
}
