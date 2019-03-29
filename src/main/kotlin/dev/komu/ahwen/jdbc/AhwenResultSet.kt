package dev.komu.ahwen.jdbc

import dev.komu.ahwen.query.Constant
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*

class AhwenResultSet(private val rows: MutableList<Map<String, Constant>>) : ResultSet {

    private var index = -1

    override fun next(): Boolean {
        if (index + 1 < rows.size) {
            index++
            return true
        } else {
            return false
        }
    }

    override fun getObject(columnLabel: String): Any {
        val value = rows[index][columnLabel] ?: error("unknown column $columnLabel")
        return value.asJavaValue()
    }

    override fun getInt(columnLabel: String): Int =
        getObject(columnLabel) as Int

    override fun getString(columnLabel: String): String =
        getObject(columnLabel) as String

    override fun close() {
    }

    override fun beforeFirst() {
        index = -1
    }

    override fun findColumn(columnLabel: String?): Int {
        error("unsupported operation")
    }

    override fun getNClob(columnIndex: Int): NClob {
        error("unsupported operation")
    }

    override fun getNClob(columnLabel: String?): NClob {
        error("unsupported operation")
    }

    override fun updateNString(columnIndex: Int, nString: String?) {
        error("unsupported operation")
    }

    override fun updateNString(columnLabel: String?, nString: String?) {
        error("unsupported operation")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) {
        error("unsupported operation")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Int) {
        error("unsupported operation")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        error("unsupported operation")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) {
        error("unsupported operation")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        error("unsupported operation")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) {
        error("unsupported operation")
    }

    override fun getStatement(): Statement {
        error("unsupported operation")
    }

    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) {
        error("unsupported operation")
    }

    override fun updateTimestamp(columnLabel: String?, x: Timestamp?) {
        error("unsupported operation")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        error("unsupported operation")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        error("unsupported operation")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) {
        error("unsupported operation")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) {
        error("unsupported operation")
    }

    override fun updateInt(columnIndex: Int, x: Int) {
        error("unsupported operation")
    }

    override fun updateInt(columnLabel: String?, x: Int) {
        error("unsupported operation")
    }

    override fun moveToInsertRow() {
        error("unsupported operation")
    }

    override fun getDate(columnIndex: Int): Date {
        error("unsupported operation")
    }

    override fun getDate(columnLabel: String?): Date {
        error("unsupported operation")
    }

    override fun getDate(columnIndex: Int, cal: Calendar?): Date {
        error("unsupported operation")
    }

    override fun getDate(columnLabel: String?, cal: Calendar?): Date {
        error("unsupported operation")
    }

    override fun getWarnings(): SQLWarning {
        error("unsupported operation")
    }

    override fun updateFloat(columnIndex: Int, x: Float) {
        error("unsupported operation")
    }

    override fun updateFloat(columnLabel: String?, x: Float) {
        error("unsupported operation")
    }

    override fun getBoolean(columnIndex: Int): Boolean {
        error("unsupported operation")
    }

    override fun getBoolean(columnLabel: String?): Boolean {
        error("unsupported operation")
    }

    override fun isFirst(): Boolean {
        error("unsupported operation")
    }

    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal {
        error("unsupported operation")
    }

    override fun getBigDecimal(columnLabel: String?, scale: Int): BigDecimal {
        error("unsupported operation")
    }

    override fun getBigDecimal(columnIndex: Int): BigDecimal {
        error("unsupported operation")
    }

    override fun getBigDecimal(columnLabel: String?): BigDecimal {
        error("unsupported operation")
    }

    override fun updateBytes(columnIndex: Int, x: ByteArray?) {
        error("unsupported operation")
    }

    override fun updateBytes(columnLabel: String?, x: ByteArray?) {
        error("unsupported operation")
    }

    override fun isLast(): Boolean {
        error("unsupported operation")
    }

    override fun insertRow() {
        error("unsupported operation")
    }

    override fun getTime(columnIndex: Int): Time {
        error("unsupported operation")
    }

    override fun getTime(columnLabel: String?): Time {
        error("unsupported operation")
    }

    override fun getTime(columnIndex: Int, cal: Calendar?): Time {
        error("unsupported operation")
    }

    override fun getTime(columnLabel: String?, cal: Calendar?): Time {
        error("unsupported operation")
    }

    override fun rowDeleted(): Boolean {
        error("unsupported operation")
    }

    override fun last(): Boolean {
        error("unsupported operation")
    }

    override fun isAfterLast(): Boolean {
        error("unsupported operation")
    }

    override fun relative(rows: Int): Boolean {
        error("unsupported operation")
    }

    override fun absolute(row: Int): Boolean {
        error("unsupported operation")
    }

    override fun getSQLXML(columnIndex: Int): SQLXML {
        error("unsupported operation")
    }

    override fun getSQLXML(columnLabel: String?): SQLXML {
        error("unsupported operation")
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        error("unsupported operation")
    }

    override fun getFloat(columnIndex: Int): Float {
        error("unsupported operation")
    }

    override fun getFloat(columnLabel: String?): Float {
        error("unsupported operation")
    }

    override fun wasNull(): Boolean {
        error("unsupported operation")
    }

    override fun getRow(): Int {
        error("unsupported operation")
    }

    override fun first(): Boolean {
        error("unsupported operation")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) {
        error("unsupported operation")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Int) {
        error("unsupported operation")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
        error("unsupported operation")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) {
        error("unsupported operation")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        error("unsupported operation")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) {
        error("unsupported operation")
    }

    override fun getURL(columnIndex: Int): URL {
        error("unsupported operation")
    }

    override fun getURL(columnLabel: String?): URL {
        error("unsupported operation")
    }

    override fun updateShort(columnIndex: Int, x: Short) {
        error("unsupported operation")
    }

    override fun updateShort(columnLabel: String?, x: Short) {
        error("unsupported operation")
    }

    override fun getType(): Int {
        error("unsupported operation")
    }

    override fun updateNClob(columnIndex: Int, nClob: NClob?) {
        error("unsupported operation")
    }

    override fun updateNClob(columnLabel: String?, nClob: NClob?) {
        error("unsupported operation")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
        error("unsupported operation")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) {
        error("unsupported operation")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?) {
        error("unsupported operation")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?) {
        error("unsupported operation")
    }

    override fun updateRef(columnIndex: Int, x: Ref?) {
        error("unsupported operation")
    }

    override fun updateRef(columnLabel: String?, x: Ref?) {
        error("unsupported operation")
    }

    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) {
        error("unsupported operation")
    }

    override fun updateObject(columnIndex: Int, x: Any?) {
        error("unsupported operation")
    }

    override fun updateObject(columnLabel: String?, x: Any?, scaleOrLength: Int) {
        error("unsupported operation")
    }

    override fun updateObject(columnLabel: String?, x: Any?) {
        error("unsupported operation")
    }

    override fun setFetchSize(rows: Int) {
        error("unsupported operation")
    }

    override fun afterLast() {
        error("unsupported operation")
    }

    override fun updateLong(columnIndex: Int, x: Long) {
        error("unsupported operation")
    }

    override fun updateLong(columnLabel: String?, x: Long) {
        error("unsupported operation")
    }

    override fun getBlob(columnIndex: Int): Blob {
        error("unsupported operation")
    }

    override fun getBlob(columnLabel: String?): Blob {
        error("unsupported operation")
    }

    override fun updateClob(columnIndex: Int, x: Clob?) {
        error("unsupported operation")
    }

    override fun updateClob(columnLabel: String?, x: Clob?) {
        error("unsupported operation")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {
        error("unsupported operation")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) {
        error("unsupported operation")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?) {
        error("unsupported operation")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?) {
        error("unsupported operation")
    }

    override fun getByte(columnIndex: Int): Byte {
        error("unsupported operation")
    }

    override fun getByte(columnLabel: String?): Byte {
        error("unsupported operation")
    }

    override fun getString(columnIndex: Int): String {
        error("unsupported operation")
    }

    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) {
        error("unsupported operation")
    }

    override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) {
        error("unsupported operation")
    }

    override fun updateDate(columnIndex: Int, x: Date?) {
        error("unsupported operation")
    }

    override fun updateDate(columnLabel: String?, x: Date?) {
        error("unsupported operation")
    }

    override fun getHoldability(): Int {
        error("unsupported operation")
    }

    override fun getObject(columnIndex: Int): Any {
        error("unsupported operation")
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any {
        error("unsupported operation")
    }

    override fun getObject(columnLabel: String?, map: MutableMap<String, Class<*>>?): Any {
        error("unsupported operation")
    }

    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T {
        error("unsupported operation")
    }

    override fun <T : Any?> getObject(columnLabel: String?, type: Class<T>?): T {
        error("unsupported operation")
    }

    override fun previous(): Boolean {
        error("unsupported operation")
    }

    override fun updateDouble(columnIndex: Int, x: Double) {
        error("unsupported operation")
    }

    override fun updateDouble(columnLabel: String?, x: Double) {
        error("unsupported operation")
    }

    override fun getLong(columnIndex: Int): Long {
        error("unsupported operation")
    }

    override fun getLong(columnLabel: String?): Long {
        error("unsupported operation")
    }

    override fun getClob(columnIndex: Int): Clob {
        error("unsupported operation")
    }

    override fun getClob(columnLabel: String?): Clob {
        error("unsupported operation")
    }

    override fun updateBlob(columnIndex: Int, x: Blob?) {
        error("unsupported operation")
    }

    override fun updateBlob(columnLabel: String?, x: Blob?) {
        error("unsupported operation")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) {
        error("unsupported operation")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) {
        error("unsupported operation")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) {
        error("unsupported operation")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?) {
        error("unsupported operation")
    }

    override fun updateByte(columnIndex: Int, x: Byte) {
        error("unsupported operation")
    }

    override fun updateByte(columnLabel: String?, x: Byte) {
        error("unsupported operation")
    }

    override fun updateRow() {
        error("unsupported operation")
    }

    override fun deleteRow() {
        error("unsupported operation")
    }

    override fun isClosed(): Boolean {
        error("unsupported operation")
    }

    override fun getNString(columnIndex: Int): String {
        error("unsupported operation")
    }

    override fun getNString(columnLabel: String?): String {
        error("unsupported operation")
    }

    override fun getCursorName(): String {
        error("unsupported operation")
    }

    override fun getArray(columnIndex: Int): Array {
        error("unsupported operation")
    }

    override fun getArray(columnLabel: String?): Array {
        error("unsupported operation")
    }

    override fun cancelRowUpdates() {
        error("unsupported operation")
    }

    override fun updateString(columnIndex: Int, x: String?) {
        error("unsupported operation")
    }

    override fun updateString(columnLabel: String?, x: String?) {
        error("unsupported operation")
    }

    override fun setFetchDirection(direction: Int) {
        error("unsupported operation")
    }

    override fun getFetchSize(): Int {
        error("unsupported operation")
    }

    override fun getCharacterStream(columnIndex: Int): Reader {
        error("unsupported operation")
    }

    override fun getCharacterStream(columnLabel: String?): Reader {
        error("unsupported operation")
    }

    override fun isBeforeFirst(): Boolean {
        error("unsupported operation")
    }

    override fun updateBoolean(columnIndex: Int, x: Boolean) {
        error("unsupported operation")
    }

    override fun updateBoolean(columnLabel: String?, x: Boolean) {
        error("unsupported operation")
    }

    override fun refreshRow() {
        error("unsupported operation")
    }

    override fun rowUpdated(): Boolean {
        error("unsupported operation")
    }

    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) {
        error("unsupported operation")
    }

    override fun updateBigDecimal(columnLabel: String?, x: BigDecimal?) {
        error("unsupported operation")
    }

    override fun getShort(columnIndex: Int): Short {
        error("unsupported operation")
    }

    override fun getShort(columnLabel: String?): Short {
        error("unsupported operation")
    }

    override fun getAsciiStream(columnIndex: Int): InputStream {
        error("unsupported operation")
    }

    override fun getAsciiStream(columnLabel: String?): InputStream {
        error("unsupported operation")
    }

    override fun updateTime(columnIndex: Int, x: Time?) {
        error("unsupported operation")
    }

    override fun updateTime(columnLabel: String?, x: Time?) {
        error("unsupported operation")
    }

    override fun getTimestamp(columnIndex: Int): Timestamp {
        error("unsupported operation")
    }

    override fun getTimestamp(columnLabel: String?): Timestamp {
        error("unsupported operation")
    }

    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp {
        error("unsupported operation")
    }

    override fun getTimestamp(columnLabel: String?, cal: Calendar?): Timestamp {
        error("unsupported operation")
    }

    override fun getRef(columnIndex: Int): Ref {
        error("unsupported operation")
    }

    override fun getRef(columnLabel: String?): Ref {
        error("unsupported operation")
    }

    override fun moveToCurrentRow() {
        error("unsupported operation")
    }

    override fun getConcurrency(): Int {
        error("unsupported operation")
    }

    override fun updateRowId(columnIndex: Int, x: RowId?) {
        error("unsupported operation")
    }

    override fun updateRowId(columnLabel: String?, x: RowId?) {
        error("unsupported operation")
    }

    override fun getNCharacterStream(columnIndex: Int): Reader {
        error("unsupported operation")
    }

    override fun getNCharacterStream(columnLabel: String?): Reader {
        error("unsupported operation")
    }

    override fun updateArray(columnIndex: Int, x: Array?) {
        error("unsupported operation")
    }

    override fun updateArray(columnLabel: String?, x: Array?) {
        error("unsupported operation")
    }

    override fun getBytes(columnIndex: Int): ByteArray {
        error("unsupported operation")
    }

    override fun getBytes(columnLabel: String?): ByteArray {
        error("unsupported operation")
    }

    override fun getDouble(columnIndex: Int): Double {
        error("unsupported operation")
    }

    override fun getDouble(columnLabel: String?): Double {
        error("unsupported operation")
    }

    override fun getUnicodeStream(columnIndex: Int): InputStream {
        error("unsupported operation")
    }

    override fun getUnicodeStream(columnLabel: String?): InputStream {
        error("unsupported operation")
    }

    override fun rowInserted(): Boolean {
        error("unsupported operation")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        error("unsupported operation")
    }

    override fun getInt(columnIndex: Int): Int {
        error("unsupported operation")
    }

    override fun updateNull(columnIndex: Int) {
        error("unsupported operation")
    }

    override fun updateNull(columnLabel: String?) {
        error("unsupported operation")
    }

    override fun getRowId(columnIndex: Int): RowId {
        error("unsupported operation")
    }

    override fun getRowId(columnLabel: String?): RowId {
        error("unsupported operation")
    }

    override fun clearWarnings() {
        error("unsupported operation")
    }

    override fun getMetaData(): ResultSetMetaData {
        error("unsupported operation")
    }

    override fun getBinaryStream(columnIndex: Int): InputStream {
        error("unsupported operation")
    }

    override fun getBinaryStream(columnLabel: String?): InputStream {
        error("unsupported operation")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        error("unsupported operation")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) {
        error("unsupported operation")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        error("unsupported operation")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        error("unsupported operation")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        error("unsupported operation")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) {
        error("unsupported operation")
    }

    override fun getFetchDirection(): Int {
        error("unsupported operation")
    }
}
