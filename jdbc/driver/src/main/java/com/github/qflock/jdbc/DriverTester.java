package com.github.qflock.jdbc;

import java.io.Console;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DriverTester {

    public static void main(String args[]) {

        if (args.length != 1) {
            System.out.println("USAGE: java [-cp driver.jar" + File.pathSeparator +
                    ".] JdbcCheck ConnectString");
            System.out.println("\nWhere\n");
            System.out.println("  ConnectString is the JDBC Connect String. Some examples:\n");
            System.out.println("    * jdbc:oracle:thin:@//server:1521/sid (Oracle DB)");
            System.out.println("    * jdbc:postgresql://server:5432/database (PostgreSQL)");
            System.out.println("\n  driver.jar is JDBC driver for the database specified in ConnectString");

            return;
        }

        String url = args[0];
        Connection con = null;
        String query = "";

        try {
            String driver = "";

            if (url.contains(":oracle:")) {
                driver = "oracle.jdbc.driver.OracleDriver";
                query = "select * from v$version";
            } else if (url.contains(":postgresql:")) {
                driver = "org.postgresql.Driver";
                query = "select version()";
            } else if (url.contains(":qflock:")) {
                driver = "com.github.qflock.jdbc.QflockDriver";
                query = "select * from tpcds.store_sales";
            } else {
                System.err.println("Don't know the driver for " + url);
                return;
            }
            System.out.println("Loading " + driver);
            Class.forName(driver).newInstance();

        } catch (Exception e) {
            System.err.println("Failed to load JDBC driver.");
            return;
        }

        try {
            Console console = System.console();

            System.out.println("Connecting to " + url);
//            String user = console.readLine("User? ");
//            char[] pass = console.readPassword("Password? ");
            System.out.println();
            Properties properties = new Properties();
            properties.setProperty("compression", "true");
            properties.setProperty("bufferSize", "42");
            con = DriverManager.getConnection(url, properties);
            Statement select = con.createStatement();
            ResultSet result = select.executeQuery(query);

            while (result.next()) System.out.println(result.getInt(1));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}