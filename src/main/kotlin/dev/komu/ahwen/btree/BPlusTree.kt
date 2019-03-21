package dev.komu.ahwen.btree

import dev.komu.ahwen.btree.Node.Internal
import dev.komu.ahwen.btree.Node.Leaf
import dev.komu.ahwen.utils.isStrictlyAscending

/**
 * An implementation of B+ tree.
 */
class BPlusTree<K, V>(private val branchingFactor: Int = 128) where K : Comparable<K> {

    private val pager = Pager<K, V>()

    fun insert(key: K, value: V) {
        val root = pager.loadRoot()
        root.insert(key, value)

        if (root.isOverflow) {
            val (splitKey, sibling) = root.split()
            val newRoot = pager.allocateInternal()
            newRoot.keys += splitKey
            newRoot.children += listOf(root.nodeId, sibling)
            pager.rootId = newRoot.nodeId
        }
    }

    fun remove(key: K): V? =
        pager.loadRoot().remove(key)

    fun dump() {
        for ((_, nodes) in nodesWithLevel().groupBy({ it.first }, { it.second })) {
            for (node in nodes) {
                print(node.keys)
                print("   ")
            }
            println()
        }
    }

    private fun nodesWithLevel(): List<Pair<Int, Node<K, V>>> {
        val result = mutableListOf<Pair<Int, Node<K, V>>>()

        fun Node<K, V>.recurse(level: Int) {
            result += level to this

            if (this is Internal)
                for (child in children)
                    pager.loadNode(child).recurse(level + 1)
        }

        pager.loadRoot().recurse(0)
        return result
    }

    val size: Int
        get() = entries().count()

    fun checkInvariants() {
        pager.loadRoot().checkInvariants(0)
    }

    operator fun get(key: K): V? =
        pager.loadRoot().findLeaf(key)[key]

    fun entries(): Sequence<Pair<K, V>> = leafs().flatMap { it.entries }

    private fun Node<K, V>.insert(key: K, value: V) {
        when (this) {
            is Internal -> {
                val child = findChild(key)
                child.insert(key, value)

                if (child.isOverflow) {
                    val (splitKey, sibling) = child.split()
                    insertChild(splitKey, sibling)
                }
                checkChildKeys()
            }
            is Leaf -> {
                val loc = keys.binarySearch(key)
                if (loc >= 0) {
                    values[loc] = value
                } else {
                    val valueIndex = -loc - 1
                    keys.add(valueIndex, key)
                    values.add(valueIndex, value)
                }
                dirty = true
            }
        }
    }

    private fun Node<K, V>.remove(key: K): V? {
        when (this) {
            is Internal -> {
                val childIndex = findChildIndex(key)
                val child = pager.loadNode(children[childIndex])
                val value = child.remove(key)

                if (child.isUnderflow)
                    rebalance(childIndex)

                return value
            }
            is Leaf -> {
                val loc = keys.binarySearch(key)
                if (loc < 0)
                    return null

                dirty = true
                keys.removeAt(loc)
                return values.removeAt(loc)
            }
            else -> error("invalid node")
        }
    }

    private fun Internal<K, V>.rebalance(childIndex: Int) {
        // TODO: avoid loading right node if left is good enough
        val left = children.getOrNull(childIndex - 1)?.let { pager.loadNode(it) }
        val right = children.getOrNull(childIndex + 1)?.let { pager.loadNode(it) }

        when {
            left != null && left.canTake -> redistribute(source = childIndex - 1, target = childIndex)
            right != null && right.canTake -> redistribute(source = childIndex + 1, target = childIndex)
            left != null -> merge(leftIndex = childIndex - 1)
            right != null -> merge(leftIndex = childIndex)
            else -> error("can't rebalance node without siblings")
        }
    }

    private fun Internal<K, V>.merge(leftIndex: Int) {
        val rightIndex = leftIndex + 1
        val left = pager.loadNode(children[leftIndex])
        val right = pager.loadNode(children[rightIndex])

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

        dirty = true
        left.dirty = true
        pager.freeNode(right.nodeId)

        if (nodeId === pager.rootId && keys.size == 0)
            pager.rootId = left.nodeId
    }

    private fun Node<K, V>.split(): Pair<K, NodeId> {
        when (this) {
            is Internal -> {
                val from = keys.size / 2 + 1
                val to = keys.size
                val movedChildren = children.subList(from, to + 1)
                val sibling = pager.allocateInternal()
                sibling.keys += keys.subList(from, to)
                sibling.children += movedChildren
                val splitKey = keys[from - 1]
                keys.subList(from - 1, to).clear()
                movedChildren.clear()
                dirty = true

                return splitKey to sibling.nodeId
            }
            is Leaf -> {
                val from = (keys.size + 1) / 2
                val to = keys.size

                val movedKeys = keys.subList(from, to)
                val movedValues = values.subList(from, to)
                val splitKey = movedKeys.first()
                val sibling = pager.allocateLeaf()
                sibling.keys += movedKeys
                sibling.values += movedValues

                movedKeys.clear()
                movedValues.clear()

                sibling.next = next
                next = sibling
                dirty = true

                return splitKey to sibling.nodeId
            }
        }
    }

    private fun Node<K, V>.checkInvariants(level: Int) {
        check(level == 0 || !isUnderflow) { "underflow $this" }
        check(!isOverflow) { "overflow at $level: $keys " }
        check(keys.isStrictlyAscending()) { "invalid keys: $keys" }

        when (this) {
            is Internal -> {
                check(children.size == keys.size + 1)

                for (child in children)
                    pager.loadNode(child).checkInvariants(level + 1)

                checkChildKeys()
            }
            is Leaf -> {
                check(values.size == keys.size) { "values.size != keys.size (${values.size} != ${keys.size})" }
            }
        }
    }

    private fun Internal<K, V>.checkChildKeys() {
        for ((i, childId) in children.withIndex()) {
            val child = pager.loadNode(childId)
            val min = keys.getOrNull(i - 1)
            val max = keys.getOrNull(i)

            if (min != null)
                check(child.keys.all { it >= min }) { "keys ${child.keys} not in range $min..$max | $keys" }

            if (max != null)
                check(child.keys.all { it < max }) { "keys ${child.keys} not in range $min..$max | $keys" }
        }
    }

    private val Node<K, V>.isOverflow: Boolean
        get() = keys.size > branchingFactor - 1

    private val Node<K, V>.isUnderflow: Boolean
        get() = when (this) {
            is Internal -> children.size < (branchingFactor + 1) / 2
            is Leaf -> values.size < branchingFactor / 2
        }

    private val Node<K, V>.canTake: Boolean
        get() = when (this) {
            is Internal -> children.size - 1 >= (branchingFactor + 1) / 2
            is Leaf -> values.size - 1 >= branchingFactor / 2
        }

    private fun leafs() = sequence<Leaf<K, V>> {
        var node: Leaf<K, V>? = pager.loadRoot().firstLeaf

        while (node != null) {
            yield(node)
            node = node.next
        }
    }

    private val Node<K, V>.firstLeaf: Leaf<K, V>
        get() = when (this) {
            is Internal -> pager.loadNode(children.first()).firstLeaf
            is Leaf -> this
        }

    /**
     * Finds the leaf node for given key. The key might not be present in the node,
     * but if any node contains the value, it will be this.
     */
    private fun Node<K, V>.findLeaf(key: K): Leaf<K, V> = when (this) {
        is Internal -> findChild(key).findLeaf(key)
        is Leaf -> this
    }

    private fun Internal<K, V>.findChild(key: K) =
        pager.loadNode(children[findChildIndex(key)])

    private fun Internal<K, V>.redistribute(source: Int, target: Int) {
        val fromNode = pager.loadNode(children[source])
        val toNode = pager.loadNode(children[target])

        val count = (fromNode.keys.size - toNode.keys.size) / 2
        assert(count > 0)

        val reversed = source < target
        val keyIndex = minOf(source, target)

        when (toNode) {
            is Internal ->
                keys[keyIndex] = toNode.moveFrom(fromNode as Internal, count, keys[keyIndex], reversed = reversed)
            is Leaf -> {
                toNode.moveFrom(fromNode as Leaf, count, reversed = reversed)
                keys[keyIndex] = pager.loadNode(children[keyIndex + 1]).keys.first()
            }
        }

        toNode.dirty = true
        fromNode.dirty = true
    }

    private fun Internal<K, V>.moveFrom(from: Internal<K, V>, count: Int, key: K, reversed: Boolean): K {
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
}
