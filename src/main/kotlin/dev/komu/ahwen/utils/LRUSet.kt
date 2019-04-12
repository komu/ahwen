package dev.komu.ahwen.utils

class LRUSet<T>() : Iterable<T> {

    private val map = LinkedHashMap<T, Unit>(128, 0.75F, true)

    constructor(values: Collection<T>): this() {
        for (value in values)
            this += value
    }

    operator fun plusAssign(value: T) {
        map[value] = Unit
    }

    fun touch(value: T) {
        map[value] // accessing the value moves it to the end of the map
    }

    fun removeEldest(): T {
        val iterator = map.keys.iterator()
        check(iterator.hasNext()) { "empty set"}
        val value = iterator.next()
        iterator.remove()
        return value
    }

    override fun iterator(): Iterator<T> =
        map.keys.iterator()
}
