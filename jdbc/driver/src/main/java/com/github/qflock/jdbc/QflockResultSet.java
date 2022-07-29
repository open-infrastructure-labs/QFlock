/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.qflock.jdbc;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.Zstd;
import com.github.qflock.jdbc.api.QFResultSet;

public class QflockResultSet implements ResultSet {
    
    final Logger logger = LoggerFactory.getLogger(QflockResultSet.class);
    
    private Queue<SQLWarning> warnings = new LinkedList<SQLWarning>();

    private QFResultSet resultset;
    public String tempDir;

    private QflockResultSetMetaData metadata;
    
    private QflockStatement statement;
    private ArrayList<ArrayList<Integer>> strOffsetVector = new ArrayList<ArrayList<Integer>>();
    private int rowIndex;
    
    private final int type = ResultSet.TYPE_SCROLL_INSENSITIVE;

    private final boolean wasNull = false;

    private boolean isClosed;

    public QflockResultSet(QFResultSet resultset,
                           String tempDir) throws SQLException {
        this.resultset = resultset;
        this.tempDir = tempDir;
        this.metadata = new QflockResultSetMetaData(resultset.metadata);
        this.rowIndex = 0;

        Integer partitions = resultset.parquet.size();
        if (partitions > 0) {
            writeResultsToPartitions();
        } else {
            getColumnResults();
        }
        this.warnings.offer(new SQLWarning("Test!"));
        this.warnings.offer(new SQLWarning("Test!"));
        this.warnings.offer(new SQLWarning("Test!"));
        this.warnings.offer(new SQLWarning("Test!"));
    }
    private void writeResultsToPartitions() throws SQLException {
        Integer partitions = resultset.parquet.size();
        Iterator<ByteBuffer> parquetIterator = this.resultset.getParquetIterator();
        Integer index = 0;
        File directory = new File(this.tempDir);
        if (! directory.exists()) {
            directory.mkdir();
            logger.info("create dir " + this.tempDir);
        } else {
            logger.info("exists " + this.tempDir);
        }
        if (!parquetIterator.hasNext()) {
            logger.info(this.tempDir + " has no files");
        } else {
            logger.info(this.tempDir + " files:" + partitions);
        }
        while (parquetIterator.hasNext()) {
            String filename = this.tempDir + "/part-" + index + ".parquet";
            try {
                logger.info("start " + filename);
                ByteBuffer bb = parquetIterator.next();
                byte[] b = new byte[bb.remaining()];
                bb.get(b);
                FileOutputStream fos = new FileOutputStream(filename);
                logger.info("start write " + filename);
                org.apache.commons.io.IOUtils.write(b, fos);
            } catch (FileNotFoundException e) {
                throw new SQLException("file not found " + filename);
            }catch (IOException e) {
                throw new SQLException("file not found " + filename);
            }
            index += 1;
        }
        logger.info("end write partitions:" + partitions);
    }
    public Integer getResultFileCount() {
        return this.resultset.parquet.size();
    }
    public Integer getSize() {
        Integer totalColBytes = 0;
        Iterator<Integer> colBytesIterator = this.resultset.getColumnBytesIterator();
        Iterator<Integer> compColBytesIterator = this.resultset.getCompressedColumnBytesIterator();
        Integer columnIndex = 0;
        while (colBytesIterator.hasNext()) {
            int colBytes = colBytesIterator.next();
            int compColBytes = compColBytesIterator.next();
            if (colBytes != compColBytes) {
                totalColBytes += compColBytes;
            } else {
                totalColBytes += colBytes;
            }
        }
        Integer columns = this.resultset.columnBytes.size();
        // Add on size of column bytes, column bytes, column type bytes (all ints)
        totalColBytes += columns * 4 * 3;
        // We (for now) exclude the size of the metadata
        return totalColBytes;
    }
    private void getColumnResults() throws SQLException {
        Iterator<Integer> colBytesIterator = this.resultset.getColumnBytesIterator();
        Iterator<Integer> compColBytesIterator = this.resultset.getCompressedColumnBytesIterator();
        Iterator<ByteBuffer> compRowsIterator = this.resultset.getCompressedRowsIterator();
        Iterator<List<Integer>> strLenListIterator = this.resultset.getStrLenVectorIterator();
        Integer columnIndex = 0;
        strOffsetVector = new ArrayList<ArrayList<Integer>>(resultset.columnBytes.size());
        while (compRowsIterator.hasNext()) {
            int colBytes = colBytesIterator.next();
            int compColBytes = compColBytesIterator.next();
            ByteBuffer compRow = compRowsIterator.next();

//            logger.info("Col " + columnIndex + "create index start");
            // Generate list of Indexes to be used when accessing strings.
            List<Integer> currentStrLen = strLenListIterator.next();
            Iterator<Integer> strLenIterator = currentStrLen.iterator();
            ArrayList<Integer> strLenArray = new ArrayList<Integer>(currentStrLen.size());
            strOffsetVector.add(strLenArray);
            Integer index = 0;
            while (strLenIterator.hasNext()) {
                int strLen = strLenIterator.next();
                strLenArray.add(index);
                index += strLen;
            }
//            logger.info("Col " + columnIndex + "create index end");
            if (colBytes != compColBytes) {
                // decompress requires a direct buffer for both source and destination.
                ByteBuffer decompressedBuffer = ByteBuffer.allocateDirect(colBytes);
                ByteBuffer compressedBuffer = ByteBuffer.allocateDirect(compColBytes);
                compressedBuffer.put(compRow);
                compressedBuffer.position(0);
                int decompressedSize = Zstd.decompress(decompressedBuffer, compressedBuffer);
                if (decompressedSize != colBytes) {
                    logger.info(String.format("colBytes: %d decompressedSize: %d",
                            colBytes, decompressedSize));
                    throw new SQLException("decompressed bytes do not match");
                }
                int type = this.metadata.getColumnType(columnIndex + 1);
                if (type == Types.VARCHAR) {
                    // Strings require access to the array() operator, so we can copy
                    // a range for each individual string.
                    // Direct buffer does not allow it, so allocate a new buffer
                    // that is not a direct buffer.
                    decompressedBuffer.position(0);
                    ByteBuffer nonDirectBuffer = ByteBuffer.allocate(colBytes);
                    nonDirectBuffer.put(decompressedBuffer);
                    nonDirectBuffer.position(0);
                    this.resultset.binaryRows.add(nonDirectBuffer);
                } else {
                    this.resultset.binaryRows.add(decompressedBuffer);
                }
                columnIndex += 1;
            } else {
                this.resultset.binaryRows.add(compRow);
            }
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean absolute(int arg0) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void afterLast() {
        // TODO Auto-generated method stub

    }

    @Override
    public void beforeFirst() {
        // TODO Auto-generated method stub

    }

    @Override
    public void cancelRowUpdates() {
        // TODO Auto-generated method stub

    }
    @Override
    protected void finalize() {
        File directory = new File(this.tempDir);
        if (directory.exists()) {
            logger.info("remove dir " + this.tempDir);
            File[] files = directory.listFiles();
            if (files != null) {
                for (File f : files) {
                    f.delete();
                }
                directory.delete();
            }
        }
    }
    @Override
    public void clearWarnings() {
        // TODO Provide warnings in ResultSet
    }

    @Override
    public void close() throws SQLException {
        this.resultset = null;
        this.isClosed = true;
        this.statement = null;
    }

    @Override
    public void deleteRow() {
        // TODO Auto-generated method stub

    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        int columnIndex = this.metadata.findColumn(columnLabel.toLowerCase());
        if (columnIndex==-1) {
            throw new SQLException();
        } else {
            return columnIndex;
        }
    }

    @Override
    public boolean first() throws SQLException {
        this.rowIndex=1;
        return true;
    }

    @Override
    public Array getArray(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Array getArray(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public InputStream getAsciiStream(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public InputStream getAsciiStream(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public BigDecimal getBigDecimal(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public BigDecimal getBigDecimal(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public InputStream getBinaryStream(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public InputStream getBinaryStream(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Blob getBlob(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Blob getBlob(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }
    public byte getByte(int colIdx, int rIndex) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public byte getByte(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public byte[] getBytes(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public byte[] getBytes(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Reader getCharacterStream(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Reader getCharacterStream(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Clob getClob(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Clob getClob(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getConcurrency() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Date getDate(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Date getDate(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Date getDate(int arg0, Calendar arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Date getDate(String arg0, Calendar arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        try {
            return this.resultset.getBinaryRows().get(columnIndex - 1)
                    .getDouble((rowIndex - 1) * 8);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to double: " + e,
                    e);
        }
    }
    public double getDouble(int columnIndex, int rIndex) throws SQLException {
        try {
            return this.resultset.getBinaryRows().get(columnIndex - 1)
                    .getDouble((rIndex - 1) * 8);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to double: " + e,
                    e);
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        try {
            return this.resultset.getBinaryRows().get(columnIndex - 1)
                    .getFloat((rowIndex - 1) * 8);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to float: " + e,
                    e);
        }
    }
    public float getFloat(int columnIndex, int rIndex) throws SQLException {
        try {
            return this.resultset.getBinaryRows().get(columnIndex - 1)
                    .getFloat((rIndex - 1) * 8);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to float: " + e,
                    e);
        }
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        try {
            return this.resultset.getBinaryRows().get(columnIndex - 1)
                    .getInt((rowIndex - 1) * 4);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to int: " + e,
                    e);
        }
    }
    public int getInt(int columnIndex, int rIndex) throws SQLException {
        try {
            return this.resultset.getBinaryRows().get(columnIndex - 1)
                    .getInt((rIndex - 1) * 4);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to int: " + e,
                    e);
        }
    }

    @Override
    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        try {
            return this.resultset.getBinaryRows().get(columnIndex - 1)
                    .getLong((rowIndex - 1) * 8);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to long: " + e,
                    e);
        }
    }
    public long getLong(int columnIndex, int rIndex) throws SQLException {
        try {
            return this.resultset.getBinaryRows().get(columnIndex - 1)
                    .getLong((rIndex - 1) * 8);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to long: " + e,
                    e);
        }
    }

    @Override
    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return this.metadata;
    }

    @Override
    public Reader getNCharacterStream(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Reader getNCharacterStream(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public NClob getNClob(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public NClob getNClob(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    public int getNumRows() {
        return this.resultset.getNumRows();
    }
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        int type = this.metadata.getColumnType(columnIndex);
        switch (type) {
        case Types.BIGINT:
            return getLong(columnIndex);
        case Types.INTEGER:
            return getInt(columnIndex);
        case Types.SMALLINT:
        case Types.TINYINT:
                return getShort(columnIndex);
        case Types.BOOLEAN:
            return getBoolean(columnIndex);
        case Types.FLOAT:
            return getFloat(columnIndex);
        case Types.DOUBLE:
            return getDouble(columnIndex);
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR:
                return getString(columnIndex);
        case Types.TIMESTAMP:
            return getTimestamp(columnIndex);
        }
        throw new SQLException("Convert from type " + type + " is not supported");
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int arg0, Map<String, Class<?>> arg1)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Object getObject(String arg0, Map<String, Class<?>> arg1)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Ref getRef(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Ref getRef(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getRow() {
        return this.rowIndex;
    }

    @Override
    public RowId getRowId(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public RowId getRowId(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public SQLXML getSQLXML(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public SQLXML getSQLXML(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        try {
            return this.resultset.getBinaryRows().get(columnIndex - 1)
                    .getShort((rowIndex - 1) * 2);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to long: " + e,
                    e);
        }
    }
    public short getShort(int columnIndex, int rIndex) throws SQLException {
        try {
            return this.resultset.getBinaryRows().get(columnIndex - 1)
                    .getShort((rIndex - 1) * 2);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to long: " + e,
                    e);
        }
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public Statement getStatement() throws SQLException {
        return this.statement;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        try {
            // Integer stringLen = this.resultset.columnTypeBytes.get(columnIndex - 1);
            Integer stringLen = this.resultset.strLenVector.get(columnIndex - 1).get(rowIndex - 1);
            Integer offset = this.strOffsetVector.get(columnIndex - 1).get(rowIndex - 1);
            byte [] buffer = this.resultset.getBinaryRows().get(columnIndex - 1)
                    .array();
            return new String(buffer, offset, stringLen, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to string: " + e,
                    e);
        }
    }
    public String getString(int columnIndex, int rIndex) throws SQLException {
        try {
//            Integer stringLen = this.resultset.columnTypeBytes.get(columnIndex - 1);
            Integer stringLen = this.resultset.strLenVector.get(columnIndex - 1).get(rIndex - 1);
            Integer offset = this.strOffsetVector.get(columnIndex - 1).get(rIndex - 1);
            byte [] buffer = this.resultset.getBinaryRows().get(columnIndex - 1)
                    .array();
            return new String(buffer, offset, stringLen, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new SQLException(
                    "Cannot convert column " + columnIndex + " to string: " + e,
                    e);
        }
    }

    @Override
    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }

    @Override
    public Time getTime(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Time getTime(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Time getTime(int arg0, Calendar arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Time getTime(String arg0, Calendar arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Timestamp getTimestamp(String arg0, Calendar arg1)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getType() throws SQLException {
        return this.type;
    }

    @Override
    public URL getURL(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public URL getURL(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public InputStream getUnicodeStream(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public InputStream getUnicodeStream(String arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public SQLWarning getWarnings() {
        return this.warnings.poll();
    }

    @Override
    public void insertRow() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isAfterLast() {
        return (getRow()>this.resultset.getNumRows());
    }

    @Override
    public boolean isBeforeFirst() {
        return (getRow()==0);
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    @Override
    public boolean isFirst() {
        return (getRow()==1);
    }

    @Override
    public boolean isLast() {
        return (getRow()==this.resultset.getNumRows());
    }

    @Override
    public boolean last() throws SQLException {
        this.rowIndex = this.resultset.getNumRows();
        return true;
    }

    @Override
    public void moveToCurrentRow() {
        // TODO Auto-generated method stub

    }

    @Override
    public void moveToInsertRow() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean next() {
        this.rowIndex += 1;
        // was .getRows().size()
        return this.rowIndex <= this.resultset.numRows;
    }

    @Override
    public boolean previous() {
        this.rowIndex -= 1;
        return this.rowIndex != 0;
    }

    @Override
    public void refreshRow() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean relative(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean rowDeleted() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean rowUpdated() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setFetchDirection(int arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFetchSize(int arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateArray(int arg0, Array arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateArray(String arg0, Array arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateAsciiStream(int arg0, InputStream arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateAsciiStream(String arg0, InputStream arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateAsciiStream(int arg0, InputStream arg1, int arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateAsciiStream(String arg0, InputStream arg1, int arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateAsciiStream(int arg0, InputStream arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateAsciiStream(String arg0, InputStream arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBigDecimal(int arg0, BigDecimal arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBigDecimal(String arg0, BigDecimal arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBinaryStream(int arg0, InputStream arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBinaryStream(String arg0, InputStream arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBinaryStream(int arg0, InputStream arg1, int arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBinaryStream(String arg0, InputStream arg1, int arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBinaryStream(int arg0, InputStream arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBinaryStream(String arg0, InputStream arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBlob(int arg0, Blob arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBlob(String arg0, Blob arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBlob(int arg0, InputStream arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBlob(String arg0, InputStream arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBlob(int arg0, InputStream arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBlob(String arg0, InputStream arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBoolean(int arg0, boolean arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBoolean(String arg0, boolean arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateByte(int arg0, byte arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateByte(String arg0, byte arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBytes(int arg0, byte[] arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateBytes(String arg0, byte[] arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateCharacterStream(int arg0, Reader arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateCharacterStream(String arg0, Reader arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateCharacterStream(int arg0, Reader arg1, int arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateCharacterStream(String arg0, Reader arg1, int arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateCharacterStream(int arg0, Reader arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateCharacterStream(String arg0, Reader arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateClob(int arg0, Clob arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateClob(String arg0, Clob arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateClob(int arg0, Reader arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateClob(String arg0, Reader arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateClob(int arg0, Reader arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateClob(String arg0, Reader arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateDate(int arg0, Date arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateDate(String arg0, Date arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateDouble(int arg0, double arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateDouble(String arg0, double arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateFloat(int arg0, float arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateFloat(String arg0, float arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateInt(int arg0, int arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateInt(String arg0, int arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateLong(int arg0, long arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateLong(String arg0, long arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNCharacterStream(int arg0, Reader arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNCharacterStream(String arg0, Reader arg1)
            {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNCharacterStream(int arg0, Reader arg1, long arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNCharacterStream(String arg0, Reader arg1, long arg2)
            {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNClob(int arg0, NClob arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNClob(String arg0, NClob arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNClob(int arg0, Reader arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNClob(String arg0, Reader arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNClob(int arg0, Reader arg1, long arg2)
            {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNClob(String arg0, Reader arg1, long arg2)
            {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNString(int arg0, String arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNString(String arg0, String arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNull(int arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNull(String arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateObject(int arg0, Object arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateObject(String arg0, Object arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateObject(int arg0, Object arg1, int arg2)
            {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateObject(String arg0, Object arg1, int arg2)
            {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateRef(int arg0, Ref arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateRef(String arg0, Ref arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateRow() {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateRowId(int arg0, RowId arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateRowId(String arg0, RowId arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateSQLXML(int arg0, SQLXML arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateSQLXML(String arg0, SQLXML arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateShort(int arg0, short arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateShort(String arg0, short arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateString(int arg0, String arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateString(String arg0, String arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateTime(int arg0, Time arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateTime(String arg0, Time arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateTimestamp(int arg0, Timestamp arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateTimestamp(String arg0, Timestamp arg1)
            {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean wasNull() {
        return this.wasNull;
    }

}
