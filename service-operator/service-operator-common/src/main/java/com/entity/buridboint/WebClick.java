package com.entity.buridboint;

import com.entity.columntype.ColumnType;

/**
 * @Classname PageView
 * @Description TODO
 * @Date 2020/2/28 12:37
 * @Created by ysh
 */
public class WebClick extends BuridBointBase{

    public WebClick() {
        properties.put("$element_id", ColumnType.STRING);
        properties.put("$element_content", ColumnType.STRING);
        properties.put("$element_name", ColumnType.STRING);
        properties.put("$element_class_name", ColumnType.STRING);
        properties.put("$element_type", ColumnType.STRING);
        properties.put("$element_selector", ColumnType.STRING);
        properties.put("$element_target_url", ColumnType.STRING);
        properties.put("$url", ColumnType.STRING);
        properties.put("$url_path", ColumnType.STRING);
        properties.put("$title", ColumnType.STRING);
        properties.put("$lib", ColumnType.STRING);
        properties.put("$lib_version", ColumnType.STRING);
        properties.put("$is_first_day", ColumnType.STRING);
        properties.put("$latest_referrer", ColumnType.STRING);
        properties.put("$latest_referrer_host", ColumnType.STRING);
        properties.put("$latest_utm_source", ColumnType.STRING);
        properties.put("$latest_utm_medium", ColumnType.STRING);
        properties.put("$latest_utm_term", ColumnType.STRING);
        properties.put("$latest_utm_content", ColumnType.STRING);
        properties.put("$latest_utm_campaign", ColumnType.STRING);
        properties.put("$latest_search_keyword", ColumnType.STRING);
        properties.put("$latest_traffic_source_type", ColumnType.STRING);
    }


}
