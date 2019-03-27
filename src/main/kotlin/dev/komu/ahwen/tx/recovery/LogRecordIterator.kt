package dev.komu.ahwen.tx.recovery

import dev.komu.ahwen.log.LogManager

class LogRecordIterator(logManager: LogManager) : Iterator<LogRecord> {

    private val iter = logManager.iterator()

    override fun hasNext(): Boolean = iter.hasNext()

    override fun next(): LogRecord {
        val record = iter.next()
        val op = record.nextInt()
        return when (op) {
            LogRecord.CHECKPOINT -> CheckPointRecord.from(record)
            LogRecord.START -> StartRecord.from(record)
            LogRecord.COMMIT -> CommitRecord.from(record)
            LogRecord.ROLLBACK -> RollbackRecord.from(record)
            LogRecord.SETINT -> SetIntRecord.from(record)
            LogRecord.SETSTRING -> SetStringRecord.from(record)
            else -> error("invalid op: $op")
        }
    }
}
