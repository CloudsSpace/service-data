package com.entity.buridboint;

import com.entity.columntype.ColumnType;

/**
 * @Classname BuridBointBaseProperties
 * @Description TODO
 * @Date 2020/2/28 12:02
 * @Created by ysh
 */
public class AppViewScreen extends BuridBointBase {

    public AppViewScreen() {
        properties.put("$title", ColumnType.STRING);
        properties.put("$screen_name",ColumnType.STRING);
    }

}
