package com.entity.buridboint;

import com.entity.columntype.ColumnType;

/**
 * @Classname BuridBointBaseProperties
 * @Description TODO
 * @Date 2020/2/28 12:02
 * @Created by ysh
 */
public class AppStart extends BuridBointBase {

    public AppStart() {
        properties.put("$resume_from_background", ColumnType.BOOLEAN);
        properties.put("$is_first_time",ColumnType.BOOLEAN);
        properties.put("$is_first_day",ColumnType.BOOLEAN);
    }

}
