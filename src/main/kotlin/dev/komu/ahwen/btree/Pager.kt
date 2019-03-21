package dev.komu.ahwen.btree

class Pager<K, V> where K : Comparable<K> {

    private val freeList = mutableListOf<NodeId>()
    private val pages = mutableListOf<Node<K, V>>()
    var rootId: NodeId = allocateLeaf().nodeId

    fun loadNode(nodeId: NodeId) =
        pages.getOrNull(nodeId.id) ?: error("could not find page $nodeId")

    fun loadRoot() =
        loadNode(rootId)

    fun freeNode(nodeId: NodeId) {
        freeList += nodeId
    }

    fun allocateInternal(): Node.Internal<K, V> =
        if (freeList.isNotEmpty()) {
            val nodeId = freeList.removeAt(freeList.lastIndex)
            val node = Node.Internal<K, V>(nodeId)
            pages[nodeId.id] = node
            node
        } else {
            val nodeId = NodeId(pages.size)
            val node = Node.Internal<K, V>(nodeId)
            pages += node
            node
        }

    fun allocateLeaf(): Node.Leaf<K, V> =
        if (freeList.isNotEmpty()) {
            val nodeId = freeList.removeAt(freeList.lastIndex)
            val node = Node.Leaf<K, V>(nodeId)
            pages[nodeId.id] = node
            node
        } else {
            val nodeId = NodeId(pages.size)
            val node = Node.Leaf<K, V>(nodeId)
            pages += node
            node
        }
}
