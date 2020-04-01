package com.kanibl.dbms.lab;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseService {

    private String jdbcString;
    private Connection connection;

    public DatabaseService(String jdbcString) throws Exception {
        this.jdbcString = jdbcString;
        connectoToDB();
    }

    public void connectoToDB() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (connection == null) {
            connection = DriverManager.getConnection(jdbcString);
        }
    }

    public void createSchema() throws SQLException {
        Statement stmt = connection.createStatement();
        try {
            stmt.executeUpdate("DROP TABLE users");
        } catch (Exception e) {
            System.out.println("Drop table failed - probably because it does not exist");
        }
        stmt.executeUpdate("CREATE TABLE users (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50))");
        System.out.println("Table user created");
        stmt.executeUpdate("INSERT INTO users VALUES (1,'John', 'Doe')");
        stmt.executeUpdate("INSERT INTO users VALUES (2,'David', 'Getta')");
        stmt.executeUpdate("INSERT INTO users VALUES (3,'Billie', 'Eilish')");
        System.out.println("3 User records inserted");
        stmt.close();
    }

    public List<Map<String, String>>  getUserAllAsMap() throws SQLException {

        String query = "SELECT * FROM users";

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
        List<Map<String,String>> data = new ArrayList<Map<String,String>>();

        for(int i = 1; i <= rsmd.getColumnCount(); i++){
            columns.add(rsmd.getColumnName(i));
        }
        while(rs.next()){
            data.add(getRsAsMap(columns,  rs));
        }
        rs.close();
        stmt.close();
        return data;
    }

    public Map<String, String> getUserById(int id) throws SQLException {

        String query = "SELECT * FROM users WHERE id = " + id;

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
        Map<String,String> data = new HashMap<String, String>();

        for(int i = 1; i <= rsmd.getColumnCount(); i++){
            columns.add(rsmd.getColumnName(i));
        }
        while(rs.next()){
            data = getRsAsMap(columns,  rs);
        }
        rs.close();
        stmt.close();
        return data;
    }

    private Map<String,String> getRsAsMap(List<String> columns, ResultSet rs) throws SQLException {
        Map<String,String> row = new HashMap<String, String>(columns.size());
        for(String col : columns) {
            row.put(col, rs.getString(col));
        }
        return row;
    }

    public void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

}
