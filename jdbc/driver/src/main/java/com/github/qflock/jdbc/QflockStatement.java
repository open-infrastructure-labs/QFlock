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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.github.qflock.jdbc.api.QFResultSet;
import com.github.qflock.jdbc.api.QFSQLException;
import com.github.qflock.jdbc.api.QFStatement;
import com.github.qflock.jdbc.api.QflockJdbcService.Client;
import com.github.qflock.jdbc.api.statement_getWarnings_return;

public class QflockStatement implements Statement {

    private QflockConnection connection;
    private QFStatement statement;

    public QflockStatement(QflockConnection connection, QFStatement stat) {
        this.connection = connection;
        this.statement = stat;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Method not supported: unwrap");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Method not supported: isWrapperFor");
        //return iface == java.sql.Statement.class || iface == CrystalStatement.class;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            QFResultSet resultset = client.statement_executeQuery(statement, sql);
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

    public void close() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            client.statement_close(statement);
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
            return client.statement_getMaxRows(statement);
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
            client.statement_setMaxRows(statement, max);
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
            return client.statement_getQueryTimeout(statement);
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
            client.statement_setQueryTimeout(statement, seconds);
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
            client.statement_cancel(statement);
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
            statement_getWarnings_return warn = client.statement_getWarnings(statement);
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
            client.statement_clearWarnings(statement);
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
            return client.statement_execute(statement, sql);
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
            QFResultSet resultset = client.statement_getResultSet(statement);
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
            return client.statement_getUpdateCount(statement);
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
        throw new SQLException("Method not supported: setFetchSize");
    }

    public int getFetchSize() throws SQLException {
        throw new SQLException("Method not supported: getFetchSize");
    }

    public int getResultSetConcurrency() throws SQLException {
        throw new SQLException("Method not supported: getResultSetConcurrency");
    }

    public int getResultSetType() throws SQLException {
        Client client = null;
        try {
            client = this.connection.lockClient();
            return client.statement_getResultSetType(statement);
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

}
