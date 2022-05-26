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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import com.github.qflock.jdbc.api.QFConnection;
import com.github.qflock.jdbc.api.QflockJdbcService;

public class QflockDriver implements Driver {
    
    final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QflockDriver.class);

    static {
        try {
          java.sql.DriverManager.registerDriver(new QflockDriver());
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url))
            return null;

        logger.info("Call to connect {} {}", url, info);
        try 
        {
            URI r = new URI("thrift:" + url.trim().substring(URL_PREFIX.length()));
            TSocket transport = new TSocket(r.getHost(), r.getPort());
            transport.open();
            logger.debug("connect, open complete {}", url);
            
            TBinaryProtocol protocol = new TBinaryProtocol(transport);
            
            QflockJdbcService.Client client = new QflockJdbcService.Client(protocol);
            Map<String, String> props = new HashMap<String, String>();
            for (Entry<Object, Object> keyEtr : info.entrySet())
            {
                props.put((String) keyEtr.getKey(), (String) keyEtr.getValue());
            }
            logger.debug("createConnection {}", url);
            QFConnection conn = client.createConnection(r.getPath(), props);
            logger.debug("createConnection complete {}", url);
            return new QflockConnection(transport, client, conn, url, info);
        } catch (TException e) {
            throw new SQLException(e);
        } catch (URISyntaxException e) {
            throw new SQLException(e);
        }
    }

    private static String URL_PREFIX = "jdbc:qflock:";
    
    public boolean acceptsURL(String url) throws SQLException {
        return Pattern.matches(URL_PREFIX + ".*", url);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {
        logger.debug("Call to DriverPropertyInfo");
        DriverPropertyInfo[] props = new DriverPropertyInfo[] {};
        return props;
    }

    public int getMajorVersion() {
        logger.debug("Call to getMajorVersion");
        return 4;
    }

    public int getMinorVersion() {
        logger.debug("Call to getMinorVersion");
        return 0;
    }

    public boolean jdbcCompliant() {
        logger.debug("Call to jdbcCompliant");
        return true;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

}
