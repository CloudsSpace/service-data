package com.entity.warehouse;


import lombok.*;

/**
 * @Author: ysh
 * @Date: 2019/8/19 21:52
 * @Version: 1.0
 */
@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class EventInfo {

    private long id;
    private long project;
    private String projectName;
    private String event;
    private String tableName;
    private String fields;
    private long status;
    private String describe;
    private String creator;
    private java.sql.Timestamp gmtCreate;
    private String modifier;
    private java.sql.Timestamp gmtModified;


}
