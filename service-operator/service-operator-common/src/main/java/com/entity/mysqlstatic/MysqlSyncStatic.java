package com.entity.mysqlstatic;

import lombok.*;

@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class MysqlSyncStatic {

    private long id;
    private String mysqlDb;
    private String mysqlTable;
    private String hiveTable;
    private long fieldId;

}
