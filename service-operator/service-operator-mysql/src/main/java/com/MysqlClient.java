package com;

import java.sql.*;
import java.util.Properties;

/**
 * mysql操作工具类
 */
public class MysqlClient {
    private static Statement stmt;

    static {
        try {
            Class.forName(Configs.key("jdbc.driver"));
            Connection conn = DriverManager.getConnection(Configs.key("jdbc.url"), Configs.key("jdbc.username"), Configs.key("jdbc.password"));
            stmt = conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static ResultSet select(int id) throws SQLException {
        String sql = String.format("select  * from product where product_id = %s", id);
        return stmt.executeQuery(sql);
    }


    public static ResultSet mysql(String sql, Properties prop) {
        String driver = "com.mysql.jdbc.Driver";
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(prop.getProperty("jdbc.url"), prop.getProperty("jdbc.user"), prop.getProperty("jdbc.password"));
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            //关闭连接 释放资源
//            if (connection != null) {
//               connection.close();
//            }
//            if (statement != null) {
//              connection.close();
//            }
        }
        return resultSet;
    }

}
