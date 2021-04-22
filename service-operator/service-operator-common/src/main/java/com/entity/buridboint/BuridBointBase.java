package com.entity.buridboint;

import com.entity.columntype.ColumnType;

import java.util.HashMap;

/**
 * @Classname BuridBointBaseProperties
 * @Description TODO
 * @Date 2020/2/28 12:02
 * @Created by ysh
 */
public class BuridBointBase {

    public  HashMap<String,ColumnType> base;

    public HashMap<String, ColumnType> properties ;

    public BuridBointBase() {
        base = new HashMap<>();
        properties = new HashMap<>();

        base.put("_flush_time", ColumnType.LONG);
        base.put("_track_id", ColumnType.LONG);

        base.put("event", ColumnType.STRING);
        base.put("distinct_id",ColumnType.STRING);
        base.put("time",ColumnType.LONG);
        base.put("project",ColumnType.STRING);

        properties.put("$device_id", ColumnType.STRING);
        properties.put("$manufacturer",ColumnType.STRING);
        properties.put("$os",ColumnType.STRING);
        properties.put("$model",ColumnType.LONG);
        properties.put("$os_version",ColumnType.STRING);
        properties.put("$app_version",ColumnType.STRING);
        properties.put("$screen_height",ColumnType.LONG);
        properties.put("$screen_width",ColumnType.LONG);


        properties.put("$carrier",ColumnType.STRING);
        properties.put("$wifi",ColumnType.BOOLEAN);
        properties.put("$network_type",ColumnType.STRING);
        properties.put("$ip",ColumnType.STRING);
        properties.put("$country",ColumnType.STRING);
        properties.put("$province",ColumnType.STRING);
        properties.put("$city",ColumnType.STRING);
    }

    public HashMap<String, ColumnType> getBase() {
        return base;
    }

    public void setBase(HashMap<String, ColumnType> base) {
        this.base = base;
    }

    public HashMap<String, ColumnType> getProperties() {
        return properties;
    }

    public void setProperties(HashMap<String, ColumnType> properties) {
        this.properties = properties;
    }
}
