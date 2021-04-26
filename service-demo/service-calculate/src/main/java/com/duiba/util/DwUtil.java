package com.duiba.util;

import com.duiba.entity.ZeusInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class DwUtil extends JDBCOperator {

    private static final String ZEUS_JOB = "demo.zeus_job";
    private Connection conn;

    public DwUtil() throws SQLException {
        initialize();
        conn = getConnection();
    }

    /**
     * 任务获取
     *
     * @param dw
     * @param id
     * @return
     * @throws SQLException
     */
    public ArrayList<ZeusInfo> getInfo( String id) throws SQLException {
        ArrayList<ZeusInfo> info = new ArrayList<>();
        String sql;
        if (id != null) {
            sql = "select id,name,script from demo.zeus_job where  id in (" + id + ")";
        } else {
            sql = "";
        }
        PreparedStatement statement = conn.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        ArrayList<ZeusInfo> whDws = putResult(resultSet, ZeusInfo.class);

        for (ZeusInfo dwEntity : whDws) {
            info.add(dwEntity);
        }
        statement.close();
        return info;
    }

    public static void main(String[] args) throws SQLException {
        ArrayList<ZeusInfo> info = new DwUtil().getInfo("1");
        System.out.println(info);
    }
}