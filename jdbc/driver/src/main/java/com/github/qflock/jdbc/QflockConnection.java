package com.github.qflock.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.qflock.jdbc.api.QFConnection;
import com.github.qflock.jdbc.api.QFSQLException;
import com.github.qflock.jdbc.api.QFStatement;
import com.github.qflock.jdbc.api.QFPreparedStatement;
import com.github.qflock.jdbc.api.QflockJdbcService.Client;

public class QflockConnection implements Connection {

    final Logger logger = LoggerFactory.getLogger(QflockConnection.class);

    QFConnection connection;
    //private Client client;
    private TSocket transport;
    private boolean isClosed;
    
    private Client client_free;
    private Client client_locked;
    private ReentrantLock transportLock = new ReentrantLock(true);
        
    public Client lockClient() {
        transportLock.lock();
        client_locked = client_free;
        return client_locked;
    }
    
    public void unlockClient(Client client) {
        client_free = client;
        client_locked = null;
        transportLock.unlock();
    }

    public QflockConnection(TSocket transport, Client client,
                            QFConnection conn, String url, Properties info) {
        this.transport = transport;
        this.client_free = client;
        connection = conn;

        this.isClosed = false;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            client.connection_setAutoCommit(connection, autoCommit);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public boolean getAutoCommit() throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            return client.connection_getAutoCommit(connection);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public void commit() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void rollback() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void close() throws SQLException {
        if (!this.isClosed)
            try {
                internalClose();
            } finally {
                this.isClosed = true;
                if (this.transport != null)
                    this.transport.close();
            }

    }

    private void internalClose() throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            client.closeConnection(connection);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return new QflockDatabaseMetaData(this);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            client.connection_setReadOnly(connection, readOnly);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public boolean isReadOnly() throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            return client.connection_getReadOnly(connection);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public void setCatalog(String catalog) throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            client.connection_setCatalog(connection, catalog);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public String getCatalog() throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            return client.connection_getCatalog(connection);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            client.connection_setTransactionIsolation(connection, level);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public int getTransactionIsolation() throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            return client.connection_getTransactionIsolation(connection);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return createPreparedStatement(sql, resultSetType, resultSetConcurrency,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return createPreparedStatement(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public int getHoldability() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Statement createStatement(int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            QFStatement statement = client.createStatement(connection);
            return new QflockStatement(this, statement);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public PreparedStatement createPreparedStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            QFPreparedStatement statement = client.createPreparedStatement(connection);
            return new QflockPreparedStatement(this, statement, sql);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Clob createClob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Blob createBlob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public NClob createNClob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public boolean isValid(int timeout) throws SQLException {
        try {
            return internalIsValid(timeout);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean internalIsValid(int timeout) throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            return client.connection_isvalid(connection, timeout);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Properties getClientInfo() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setSchema(String schema) throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            client.connection_setSchema(connection, schema);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public String getSchema() throws SQLException {
        Client client = null;
        try {
            client = this.lockClient();
            return client.connection_getSchema(connection);
        } catch (QFSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.unlockClient(client);
        }
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLException("Method not supported");
    }

}
