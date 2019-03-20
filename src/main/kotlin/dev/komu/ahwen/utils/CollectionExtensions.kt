package dev.komu.ahwen.utils

fun <T : Comparable<T>> Iterable<T>.isStrictlyAscending(): Boolean {
    var previous: T? = null

    for (value in this) {
        if (previous != null && value <= previous)
            return false
        previous = value
    }

    return true
}
