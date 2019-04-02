package dev.komu.ahwen.multibuffer

import kotlin.math.ceil
import kotlin.math.pow

object BufferNeeds {
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

    fun bestFactor(size: Int, available: Int): Int {
        if (available <= 1)
            return 1
        var k = size
        var i = 1.0
        while (k > available) {
            i++
            k = ceil(size / i).toInt()
        }
        return k
    }
}
