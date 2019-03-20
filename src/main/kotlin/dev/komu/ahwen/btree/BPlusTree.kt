package dev.komu.ahwen.btree

import dev.komu.ahwen.utils.isStrictlyAscending

/**
 * An implementation of B+ tree.
 */
class BPlusTree<K, V>(private val branchingFactor: Int = 128) where K : Comparable<K> {

    private var root: Node = Leaf(emptyList(), emptyList())

    fun insert(key: K, value: V) {
        root.insert(key, value)

        if (root.isOverflow) {
            val (splitKey, sibling) = root.split()
            root = Internal(
                keys = listOf(splitKey),
                children = listOf(root, sibling)
            )
        }
    }

    fun remove(key: K) {
        root.remove(key)
    }

    fun dump() {
        val nodesWithLevel = mutableListOf<Pair<Int, Node>>()
        root.collectNodes(0, nodesWithLevel)

        for ((_, nodes) in nodesWithLevel.groupBy({ it.first }, { it.second })) {
            for (node in nodes) {
                print(node.keys)
                print("   ")
            }
            println()
        }
    }

    fun countEntries(): Int {
        val nodesWithLevel = mutableListOf<Pair<Int, Node>>()
        root.collectNodes(0, nodesWithLevel)
        return nodesWithLevel.map { it.second }.filterIsInstance<Leaf>().sumBy { it.keys.size }
    }

    fun checkInvariants() {
        root.checkInvariants(0)
    }

    operator fun get(key: K): V? =
        root.findLeaf(key)[key]

    fun entries(): Iterator<Pair<K, V>> = iterator {
        var node: Leaf? = root.firstLeaf

        while (node != null) {
            for ((i, key) in node.keys.withIndex())
                yield(key to node.values[i])

            node = node.next
        }
    }

    private abstract inner class Node {

        val keys = mutableListOf<K>()

        val isOverflow: Boolean
            get() = keys.size > branchingFactor - 1

        abstract val isUnderflow: Boolean
        abstract val canTake: Boolean
        abstract val firstLeaf: Leaf

        /**
         * Finds the leaf node for given key. The key might not be present in the node,
         * but if any node contains the value, it will be this.
         */
        abstract fun findLeaf(key: K): Leaf

        abstract fun insert(key: K, value: V)
        abstract fun checkInvariants(level : Int)

        protected fun findChildIndex(key: K): Int {
            val loc = keys.binarySearch(key)
            return if (loc >= 0) loc + 1 else -loc - 1
        }

        abstract fun collectNodes(level: Int, result: MutableList<Pair<Int, Node>>)
        abstract fun split(): Pair<K, Node>
        abstract fun remove(key: K)
    }

    private inner class Internal(keys: List<K>, children: List<Node>): Node() {
        private val children = mutableListOf<Node>()

        init {
            check(keys.size + 1 == children.size)
            this.keys += keys
            this.children += children
        }

        override val firstLeaf: Leaf
            get() = children[0].firstLeaf

        override fun checkInvariants(level: Int) {
            check(level == 0 || !isUnderflow) { "underflow (children=${children.size})"}
            check(!isOverflow) { "overflow (children=${children.size})"}
            check(children.size == keys.size + 1)
            check(keys.isStrictlyAscending()) { "invalid keys: $keys" }

            for (child in children)
                child.checkInvariants(level + 1)

            checkChildKeys()
        }

        private fun checkChildKeys() {
            for ((i, child) in children.withIndex()) {
                val min = keys.getOrNull(i - 1)
                val max = keys.getOrNull(i)

                if (min != null)
                    check(child.keys.all { it >= min }) { "keys ${child.keys} not in range $min..$max | $keys" }

                if (max != null)
                    check(child.keys.all { it < max }) { "keys ${child.keys} not in range $min..$max | $keys" }
            }
        }

        private fun merge(right: Internal, key: K) {
            keys += key
            keys += right.keys
            children += right.children
        }

        override fun toString(): String = "internal $keys"

        override fun findLeaf(key: K) =
            findChild(key).findLeaf(key)

        override val isUnderflow: Boolean
            get() = children.size < (branchingFactor + 1) / 2

        override val canTake: Boolean
            get() = children.size - 1 >= (branchingFactor + 1) / 2

        override fun insert(key: K, value: V) {
            val child = findChild(key)
            child.insert(key, value)

            if (child.isOverflow) {
                val (splitKey, sibling) = child.split()
                insertChild(splitKey, sibling)
            }
            checkChildKeys()
        }

        private fun insertChild(key: K, child: Node) {
            val loc = keys.binarySearch(key)
            if (loc >= 0) {
                // children[loc + 1] = child
                error("trying to insert duplicate child")
            } else {
                val index = -loc - 1
                keys.add(index, key)
                children.add(index + 1, child)
            }
        }

        override fun remove(key: K) {
            val childIndex = findChildIndex(key)
            val child = children[childIndex]
            child.remove(key)

            if (child.isUnderflow)
                rebalance(childIndex)

//            checkInvariants(if (this == root) 0 else 1)
        }

        private fun rebalance(childIndex: Int) {
            val left = children.getOrNull(childIndex - 1)
            val right = children.getOrNull(childIndex + 1)

            when {
                left != null && left.canTake -> redistribute(source = childIndex - 1, target = childIndex)
                right != null && right.canTake -> redistribute(source = childIndex + 1, target = childIndex)
                left != null -> merge(leftIndex = childIndex - 1)
                right != null -> merge(leftIndex = childIndex)
                else -> error("can't rebalance node without siblings")
            }
        }

        private fun merge(leftIndex: Int) {
            val rightIndex = leftIndex + 1
            val left = children[leftIndex]
            val right = children[rightIndex]

            check(left.keys.size + right.keys.size <= branchingFactor) { "merge $left+$right > $branchingFactor" }

            val key = keys.removeAt(rightIndex - 1)
            children.removeAt(rightIndex)

            when (left) {
                is Internal -> left.merge(right as Internal, key)
                is Leaf -> left.merge(right as Leaf)
            }

            if (left.isOverflow) {
                assert(left is Internal)
                val (splitKey, sibling) = left.split()
                insertChild(splitKey, sibling)
            }

            if (this === root && keys.size == 0)
                root = left
        }

        private fun redistribute(source: Int, target: Int) {
            val fromNode = children[source]
            val toNode = children[target]

            val count = (fromNode.keys.size - toNode.keys.size) / 2
            assert(count > 0)

            val reversed = source < target
            val keyIndex = minOf(source, target)

            when (toNode) {
                is Internal ->
                    keys[keyIndex] = toNode.moveFrom(fromNode as Internal, count, keys[keyIndex], reversed = reversed)
                is Leaf -> {
                    toNode.moveFrom(fromNode as Leaf, count, reversed = reversed)
                    keys[keyIndex] = children[keyIndex + 1].keys.first()
                }
            }
        }

        private fun moveFrom(from: Internal, count: Int, key: K, reversed: Boolean): K {
            if (!reversed) {
                val takenKeys = from.keys.subList(0, count)
                val resultKey = takenKeys.last()

                keys.addAll(listOf(key) + takenKeys.subList(0, takenKeys.size - 1))

                takenKeys.clear()

                val takenChildren = from.children.subList(0, count)
                children.addAll(takenChildren)
                takenChildren.clear()

                return resultKey

            } else {
                val takenKeys = from.keys.subList(from.keys.size - count, from.keys.size)
                val resultKey = takenKeys.first()

                keys.addAll(0, takenKeys.subList(1, takenKeys.size) + key)

                takenKeys.clear()

                val takenChildren = from.children.subList(from.children.size - count, from.children.size)
                children.addAll(0, takenChildren)
                takenChildren.clear()

                return resultKey
            }
        }

        private fun findChild(key: K) =
            children[findChildIndex(key)]

        override fun collectNodes(level: Int, result: MutableList<Pair<Int, Node>>) {
            result += level to this

            for (child in children)
                child.collectNodes(level + 1, result)
        }

        override fun split(): Pair<K, Node> {
            val from = keys.size / 2 + 1
            val to = keys.size
            val movedChildren = children.subList(from, to + 1)
            val sibling = Internal(
                keys = keys.subList(from, to),
                children = movedChildren
            )
            val splitKey = keys[from - 1]
            keys.subList(from - 1, to).clear()
            movedChildren.clear()

            return splitKey to sibling
        }
    }

    private inner class Leaf(keys: List<K>, values: List<V>): Node() {
        val values = mutableListOf<V>()
        var next: Leaf? = null

        init {
            this.keys += keys
            this.values += values
        }

        override val isUnderflow: Boolean
            get() = values.size < branchingFactor / 2

        override val canTake: Boolean
            get() = values.size - 1 >= branchingFactor / 2

        override val firstLeaf: Leaf
            get() = this

        override fun findLeaf(key: K) = this

        override fun checkInvariants(level: Int) {
            check(level == 0 || !isUnderflow) { "underflow at $level: $keys"}
            check(!isOverflow) { "overflow at $level: $keys "}
            check(values.size == keys.size)
            check(keys.isStrictlyAscending()) { "invalid keys: $keys" }
        }

        operator fun get(key: K): V? {
            val index = keys.binarySearch(key)
            if (index < 0)
                return null

            return values[index]
        }

        fun merge(right: Leaf) {
            keys += right.keys
            values += right.values
            next = right.next
        }

        override fun insert(key: K, value: V) {
            val loc = keys.binarySearch(key)
            if (loc >= 0) {
                values[loc] = value
            } else {
                val valueIndex = -loc - 1
                keys.add(valueIndex, key)
                values.add(valueIndex, value)
            }
        }

        override fun remove(key: K) {
            val loc = keys.binarySearch(key)
            if (loc >= 0) {
                keys.removeAt(loc)
                values.removeAt(loc)
            }
        }

        override fun collectNodes(level: Int, result: MutableList<Pair<Int, Node>>) {
            result += level to this
        }

        fun moveFrom(from: Leaf, count: Int, reversed: Boolean) {
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

        override fun split(): Pair<K, Node> {
            val from = (keys.size + 1) / 2
            val to = keys.size

            val movedKeys = keys.subList(from, to)
            val movedValues = values.subList(from, to)
            val splitKey = movedKeys.first()
            val sibling = Leaf(movedKeys, movedValues)

            movedKeys.clear()
            movedValues.clear()

            sibling.next = next
            next = sibling
            return splitKey to sibling
        }

        override fun toString(): String = "leaf $keys"
    }
}
