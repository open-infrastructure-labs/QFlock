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

import com.github.qflock.jdbc.api.QflockJdbcService.Client;
import com.github.qflock.jdbc.api.QFResultSet;
import com.github.qflock.jdbc.api.QFSQLException;
import com.github.qflock.jdbc.api.QFPreparedStatement;
import com.github.qflock.jdbc.api.preparedStatement_getWarnings_return;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

public class QflockPreparedStatement implements PreparedStatement {

    private QflockConnection connection;
    private QFPreparedStatement statement;
    private String sql;
    private Integer fetchSize;

    public QflockPreparedStatement(QflockConnection connection, QFPreparedStatement stat,
                                   String sql) {
        this.connection = connection;
        this.statement = stat;
        this.sql = sql;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Method not supported: unwrap");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Method not supported: isWrapperFor");
        //return iface == java.sql.Statement.class || iface == CrystalStatement.class;
    }
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(this.sql);
    }
    public ResultSet executeQuery(String sql) throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            QFResultSet resultset = client.preparedStatement_executeQuery(statement, sql);
            return new QflockResultSet(resultset, this.connection.getClientInfo("tempDir"));
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Method not supported: executeUpdate");
    }
    public int executeUpdate() throws SQLException {
        return executeUpdate(this.sql);
    }

    public void close() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            client.preparedStatement_close(statement);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public int getMaxFieldSize() throws SQLException {
        throw new SQLException("Method not supported: getMaxFieldSize");
    }

    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLException("Method not supported: setMaxFieldSize");
    }

    public int getMaxRows() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            return client.preparedStatement_getMaxRows(statement);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public void setMaxRows(int max) throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            client.preparedStatement_setMaxRows(statement, max);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLException("Method not supported: setEscapeProcessing");
    }

    public int getQueryTimeout() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            return client.preparedStatement_getQueryTimeout(statement);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            client.preparedStatement_setQueryTimeout(statement, seconds);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public void cancel() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            client.preparedStatement_cancel(statement);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            preparedStatement_getWarnings_return warn = client.preparedStatement_getWarnings(statement);
            return QflockWarning.buildFromList(warn.warnings);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public void clearWarnings() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            client.preparedStatement_clearWarnings(statement);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public void setCursorName(String name) throws SQLException {
        throw new SQLException("Method not supported: setCursorName");
    }

    public boolean execute(String sql) throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            return client.preparedStatement_execute(statement, sql);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public ResultSet getResultSet() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            QFResultSet resultset = client.preparedStatement_getResultSet(statement);
            return new QflockResultSet(resultset, this.connection.getClientInfo("tempDir"));
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public int getUpdateCount() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            return client.preparedStatement_getUpdateCount(statement);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public boolean getMoreResults() throws SQLException {
        throw new SQLException("Method not supported: getMoreResults");
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException("Method not supported: setFetchDirection");
    }

    public int getFetchDirection() throws SQLException {
        throw new SQLException("Method not supported: getFetchDirection");
    }

    public void setFetchSize(int rows) throws SQLException {
        fetchSize = rows;
    }

    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    public int getResultSetConcurrency() throws SQLException {
        throw new SQLException("Method not supported: getResultSetConcurrency");
    }

    public int getResultSetType() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            return client.preparedStatement_getResultSetType(statement);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.connection.unlockClient(client);
        }
    }

    public void addBatch(String sql) throws SQLException {
        throw new SQLException("Method not supported: addBatch");
    }

    public void clearBatch() throws SQLException {
        throw new SQLException("Method not supported: clearBatch");
    }

    public int[] executeBatch() throws SQLException {
        throw new SQLException("Method not supported: executeBatch");
    }

    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLException("Method not supported: getMoreResults");
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("Method not supported: getGeneratedKeys");
    }

    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLException("Method not supported:executeUpdate");
    }

    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        throw new SQLException("Method not supported: executeUpdate");
    }

    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLException("Method not supported: executeUpdate");
    }

    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLException("Method not supported: execute");
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Method not supported: execute");
    }

    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLException("Method not supported: execute");
    }

    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Method not supported: getResultSetHoldability");
    }

    public boolean isClosed() throws SQLException {
        throw new SQLException("Method not supported: isClosed");
    }

    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLException("Method not supported: setPoolable");
    }

    public boolean isPoolable() throws SQLException {
        throw new SQLException("Method not supported: isPoolable");
    }

    public void closeOnCompletion() throws SQLException {
        throw new SQLException("Method not supported: closeOnCompletion");
    }

    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLException("Method not supported: isCloseOnCompletion");
    }

    public void setNull(int var1, int var2) throws SQLException {}

    public void setBoolean(int var1, boolean var2) throws SQLException {}

    public void setByte(int var1, byte var2) throws SQLException {}

    public void setShort(int var1, short var2) throws SQLException {}

    public void setInt(int var1, int var2) throws SQLException {}

    public void setLong(int var1, long var2) throws SQLException {}

    public void setFloat(int var1, float var2) throws SQLException {}

    public void setDouble(int var1, double var2) throws SQLException {}

    public void setBigDecimal(int var1, BigDecimal var2) throws SQLException {}

    public void setString(int var1, String var2) throws SQLException {}

    public void setBytes(int var1, byte[] var2) throws SQLException {}

    public void setDate(int var1, Date var2) throws SQLException {}

    public void setTime(int var1, Time var2) throws SQLException {}

    public void setTimestamp(int var1, Timestamp var2) throws SQLException {}

    public void setAsciiStream(int var1, InputStream var2, int var3) throws SQLException {}

    public void setUnicodeStream(int var1, InputStream var2, int var3) throws SQLException {}

    public void setBinaryStream(int var1, InputStream var2, int var3) throws SQLException {}

    public void clearParameters() throws SQLException {}

    public void setObject(int var1, Object var2, int var3) throws SQLException {}

    public void setObject(int var1, Object var2) throws SQLException {}

    public boolean execute() throws SQLException {
        throw new SQLException("Method not supported: execute()");
    }

    public void addBatch() throws SQLException {}

    public void setCharacterStream(int var1, Reader var2, int var3) throws SQLException {}

    public void setRef(int var1, Ref var2) throws SQLException {}

    public void setBlob(int var1, Blob var2) throws SQLException {}

    public void setClob(int var1, Clob var2) throws SQLException {}

    public void setArray(int var1, Array var2) throws SQLException {}

    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("Method not supported: getMetaData()");
    }

    public void setDate(int var1, Date var2, Calendar var3) throws SQLException {}

    public void setTime(int var1, Time var2, Calendar var3) throws SQLException {}

    public void setTimestamp(int var1, Timestamp var2, Calendar var3) throws SQLException {}

    public void setNull(int var1, int var2, String var3) throws SQLException {}

    public void setURL(int var1, URL var2) throws SQLException {}

    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLException("Method not supported: getParameterMetaData()");
    }

    public void setRowId(int var1, RowId var2) throws SQLException {}

    public void setNString(int var1, String var2) throws SQLException {}

    public void setNCharacterStream(int var1, Reader var2, long var3) throws SQLException {}

    public void setNClob(int var1, NClob var2) throws SQLException {}

    public void setClob(int var1, Reader var2, long var3) throws SQLException {}

    public void setBlob(int var1, InputStream var2, long var3) throws SQLException {}

    public void setNClob(int var1, Reader var2, long var3) throws SQLException {}

    public void setSQLXML(int var1, SQLXML var2) throws SQLException {}

    public void setObject(int var1, Object var2, int var3, int var4) throws SQLException {}

    public void setAsciiStream(int var1, InputStream var2, long var3) throws SQLException {}

    public void setBinaryStream(int var1, InputStream var2, long var3) throws SQLException {}

    public void setCharacterStream(int var1, Reader var2, long var3) throws SQLException {}

    public void setAsciiStream(int var1, InputStream var2) throws SQLException {}

    public void setBinaryStream(int var1, InputStream var2) throws SQLException {}

    public void setCharacterStream(int var1, Reader var2) throws SQLException {}

    public void setNCharacterStream(int var1, Reader var2) throws SQLException {}

    public void setClob(int var1, Reader var2) throws SQLException {}

    public void setBlob(int var1, InputStream var2) throws SQLException {}

    public void setNClob(int var1, Reader var2) throws SQLException {}
}
