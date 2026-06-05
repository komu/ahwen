package dev.komu.ahwen.utils

import java.time.Duration
import java.util.concurrent.locks.Condition

fun Condition.await(duration: Duration) =
    awaitNanos(duration.toNanos())
