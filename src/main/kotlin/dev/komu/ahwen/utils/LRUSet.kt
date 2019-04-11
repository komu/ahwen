package dev.komu.ahwen.utils

class LRUSet<T>() : Iterable<T> {

    private var newestLink: Link<T>? = null
    private var eldestLink: Link<T>? = null
    private val map = mutableMapOf<T, Link<T>>()

    constructor(values: Collection<T>): this() {
        for (value in values)
            this += value
    }

    operator fun plusAssign(value: T) {
        val link = Link(value, newestLink)
        newestLink = link
        map[value] = link

        if (eldestLink == null)
            eldestLink = link
    }

    fun touch(value: T) {
        val link = map[value] ?: return

        if (link == eldestLink)
            eldestLink = link.newer

        link.older?.newer = link.newer
        link.newer?.older = link.older

        link.newer = null
        link.older = newestLink

        newestLink?.newer = link
        newestLink = link

        if (eldestLink == null)
            eldestLink = link
    }

    fun removeEldest(): T {
        val link = eldestLink ?: error("empty lru")

        map.remove(link.value)

        if (newestLink == link) {
            eldestLink = null
            newestLink = null
        } else {
            eldestLink = link.newer
            link.newer?.older = null
        }

        return link.value
    }

    override fun iterator(): Iterator<T> = iterator {
        var link = eldestLink
        while (link != null) {
            yield(link.value)
            link = link.newer
        }
    }

    val eldest: T?
        get() = eldestLink?.value

    private class Link<T>(val value: T, var older: Link<T>?) {
        var newer : Link<T>? = null

        init {
            older?.newer = this
        }
    }
}
