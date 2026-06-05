package dev.komu.ahwen.log

/**
 * Log sequence number
 *
 * Can be used to guarantee that log is flushed until certain point in time.
 */
data class LSN(val lsn: Int) : Comparable<LSN> {

    override fun compareTo(other: LSN): Int =
        compareValues(lsn, other.lsn)

    companion object {
        val zero = LSN(0)
        val undefined = LSN(-1)
    }
}
