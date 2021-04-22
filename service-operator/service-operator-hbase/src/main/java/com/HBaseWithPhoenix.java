package com;

import java.sql.*;

/**
 * @Author: Yang JianQiu
 * @Date: 2019/2/14 15:41
 *
 *  java的jdbc
 * 参考资料：
 * https://www.cnblogs.com/linbingdong/p/5832112.html
 * https://cloud.tencent.com/developer/article/1032855
 * https://www.cnblogs.com/MOBIN/p/5467284.html
 */
public class HBaseWithPhoenix {
    public static void main(String[] args) {
        jdbcWithPhoenix();
    }
    public static void jdbcWithPhoenix() {
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try {
            conn = DriverManager.getConnection("jdbc:phoenix:192.168.187.201:2181");
            //stmt = conn.createStatement();

            //stmt.executeUpdate("create table \"test\" (id varchar primary key, \"cf1\".\"name\" varchar, \"cf1\".\"age\" varchar )");
            //stmt.executeUpdate("upsert into \"test\" values (1, 'hello', '25')");
            //stmt.executeUpdate("upsert into \"test\" values (2, 'world', '26')");
            //conn.commit();

            //table小写需要加双引号
            ps = conn.prepareStatement("select * from \"test\"");
            resultSet = ps.executeQuery();
            while (resultSet.next()){
                System.out.println(resultSet.getString("id") + " " + resultSet.getString("name") + " " + resultSet.getString("age"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null){
                    stmt.close();
                }
                if (ps != null){
                    ps.close();
                }

                if (conn != null){
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}