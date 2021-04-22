package com.entity.buridboint;


import com.entity.columntype.ColumnType;

/**
 * @Classname BuridBointBaseProperties
 * @Description TODO
 * @Date 2020/2/28 12:02
 * @Created by ysh
 */
public class AppClick extends BuridBointBase {

    public AppClick() {
        properties.put("$title", ColumnType.STRING);
        properties.put("$screen_name",ColumnType.STRING);
        properties.put("$element_content",ColumnType.STRING);
        properties.put("$element_position",ColumnType.STRING);
        properties.put("$element_id",ColumnType.STRING);
        properties.put("$element_type",ColumnType.STRING);
    }

}
