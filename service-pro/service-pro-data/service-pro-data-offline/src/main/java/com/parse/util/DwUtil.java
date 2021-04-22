package com.parse.util;

import com.JDBCOperator;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.entity.buridboint.AppStart;
import com.entity.columntype.ColumnType;
import com.entity.warehouse.DwEntity;
import com.entity.warehouse.EventInfo;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Author: ysh
 * @Date: 2019/10/15 14:58
 * @Version: 1.0
 */
public class DwUtil extends JDBCOperator {

    private static final String EVENT_TABLE = "bidb.dw_ods";

    private static final String MYSQL_LOAD_INFO = "bidb.mysql_load_info";
    private Connection conn;

    public DwUtil() throws SQLException {
        initialize();
        conn = getConnection();
    }

    public HashMap<String, EventInfo> getEventInfoMap() throws SQLException {
        PreparedStatement statement = conn.prepareStatement("select * from " + EVENT_TABLE + " where status = 1");
        ResultSet resultSet = statement.executeQuery();
        HashMap<String, EventInfo> hashMap = new HashMap<String, EventInfo>();
        ArrayList<EventInfo> eventInfos = putResult(resultSet, EventInfo.class);
        for (EventInfo eventInfo : eventInfos) {
            String fileName = eventInfo.getProjectName() + "_" + eventInfo.getEvent();
            hashMap.put(fileName, eventInfo);
        }
        return hashMap;
    }

    public HashMap<Long, ArrayList<DwEntity>> getDwInfo(String dw) throws SQLException {
        HashMap<Long, ArrayList<DwEntity>> dws = new HashMap<>();
        PreparedStatement statement = conn.prepareStatement("select * from bidb.dw_" + dw + " order by exec_level");
        ResultSet resultSet = statement.executeQuery();
        ArrayList<DwEntity> whDws = putResult(resultSet, DwEntity.class);

        for (DwEntity dwEntity : whDws) {
            long execLevel = dwEntity.getExecLevel();
            if (execLevel >= 0) {
                ArrayList<DwEntity> dwEntities = dws.get(dwEntity.getExecLevel());
                if (dwEntities == null) {
                    dwEntities = new ArrayList<>();
                }
                dwEntities.add(dwEntity);
                dws.put(dwEntity.getExecLevel(), dwEntities);
            }
        }

        statement.close();
        return dws;
    }


    public ArrayList<DwEntity> getMysqlLoadInfo(String ids) throws SQLException {
        PreparedStatement statement;
        if (ids != null) {
            statement = conn.prepareStatement("select * from " + MYSQL_LOAD_INFO + " where id in (" + ids + ")");
        } else {
            statement = conn.prepareStatement("select * from " + MYSQL_LOAD_INFO);
        }
        ResultSet resultSet = statement.executeQuery();
        ArrayList<DwEntity> mysqlLoadInfos = putResult(resultSet, DwEntity.class);

        statement.close();
        return mysqlLoadInfos;
    }


    public JSONObject convertFields(String fields) {
        return JSON.parseObject(fields, Feature.OrderedField);
    }


    /**
     * 新增埋点字段
     *
     * @param id
     * @throws SQLException
     */
    public void addField(int id) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("select * from bidb.dw_ods_tmp where id = " + id);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            JSONObject jsonObject = JSON.parseObject(resultSet.getString(5), Feature.OrderedField);

            jsonObject.put("properties=assessment_level", "StringType");

            PreparedStatement preparedStatement1 = conn.prepareStatement("update bidb.dw_ods_tmp set fields = ? where id = ?");
            preparedStatement1.setString(1, jsonObject.toJSONString());
            preparedStatement1.setInt(2, id);
            preparedStatement1.executeUpdate();

        }
    }


    /**
     * 新增埋点事件
     *
     * @param project
     * @param projectName
     * @param event
     */
    public static void addEvent(int project, String projectName, String event, HashMap<String, ColumnType> map) {
        String sql = "INSERT INTO bidb.dw_ods_back (project, project_name, event, table_name, fields, status) VALUES (";
        String tableName = projectName + "_" + event.toLowerCase().replace("$", "e_");
        int status = 1;
        String fields = getFields(map);
        sql = sql + project + ",'" + projectName + "','" + event + "','" + tableName + "','" + fields + "'," + status + ");";
        System.out.println(sql);
    }

    private static String getFields(HashMap<String, ColumnType> map) {
        AppStart appStart = new AppStart();
        JSONObject baseObj = new JSONObject(true);
        JSONObject proObj = new JSONObject(true);

        Set<Map.Entry<String, ColumnType>> base = appStart.base.entrySet();
        for (Map.Entry<String, ColumnType> entry : base) {
            baseObj.put("event=" + entry.getKey(), entry.getValue().getType());
        }

        Set<Map.Entry<String, ColumnType>> properties = appStart.properties.entrySet();
        for (Map.Entry<String, ColumnType> entry : properties) {
            proObj.put("properties=" + entry.getKey(), entry.getValue().getType());
        }

        if (map != null && map.size() != 1) {
            Set<String> strings = map.keySet();
            for (String key : strings) {
                proObj.put("properties=" + key, map.get(key).getType());
            }
        }


        baseObj.putAll(proObj);
        return JSONObject.toJSONString(baseObj);
    }

    public static void main(String[] args) {
        HashMap<String, ColumnType> map = new HashMap<>();
        map.put("click_location", ColumnType.STRING);
        map.put("is_feedback", ColumnType.BOOLEAN);
        addEvent(1, "production", "explain_feedback_click", map);

        HashMap<String, ColumnType> map2 = new HashMap<>();
        map.put("show_location", ColumnType.STRING);
        map.put("nterbehavior", ColumnType.STRING);
        addEvent(1, "production", "member_pop_vip_contents", map2);

        HashMap<String, ColumnType> map3 = new HashMap<>();
        map.put("show_location", ColumnType.STRING);
        map.put("nterbehavior", ColumnType.STRING);
        addEvent(1, "production", "member_pop_download", map3);

        HashMap<String, ColumnType> map4 = new HashMap<>();
        map.put("show_location", ColumnType.STRING);
        map.put("nterbehavior", ColumnType.STRING);
        addEvent(1, "production", "member_pop_ai", map4);

        HashMap<String, ColumnType> map5 = new HashMap<>();
        map.put("click_location", ColumnType.STRING);
        map.put("album_id", ColumnType.STRING);
        map.put("album_title", ColumnType.STRING);
        addEvent(1, "production", "home_page_like_list_click", map5);

        HashMap<String, ColumnType> map6 = new HashMap<>();
        map.put("click_location", ColumnType.STRING);
        map.put("album_id", ColumnType.STRING);
        map.put("album_title", ColumnType.STRING);
        addEvent(1, "production", "home_page_like_click", map6);

        HashMap<String, ColumnType> map7 = new HashMap<>();
        map.put("click_location", ColumnType.STRING);
        map.put("share_channels", ColumnType.STRING);
        map.put("hide_score", ColumnType.BOOLEAN);
        addEvent(1, "production", "publish_page_click", map7);
    }


}