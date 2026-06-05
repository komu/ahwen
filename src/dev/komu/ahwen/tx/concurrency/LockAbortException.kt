package dev.komu.ahwen.tx.concurrency

/**
 * Exception thrown when locking fails because of a timeout or deadlock.
 */
class LockAbortException : RuntimeException()
