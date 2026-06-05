package dev.komu.ahwen.utils

import java.nio.ByteBuffer

val ByteBuffer.isZeroed: Boolean
    get() = (0 until capacity()).all { get(it) == 0.toByte() }
