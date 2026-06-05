package dev.komu.ahwen.query

import kotlin.math.ceil
import kotlin.math.pow

/**
 * Returns the best root of [size] that is smaller than equal to [available].
 */
fun bestRoot(size: Int, available: Int): Int {
    if (available <= 1)
        return 1
    var k = Int.MAX_VALUE
    var i = 1.0
    while (k > available) {
        i++
        k = ceil(size.toDouble().pow(1 / i)).toInt()
    }

    return k
}

/**
 * Returns greatest factor of [size] that is smaller or equal to [available].
 */
fun bestFactor(size: Int, available: Int): Int {
    if (available <= 1)
        return 1

    var k = size
    var i = 1
    while (k > available) {
        i++
        k = size / i
    }
    return k
}
