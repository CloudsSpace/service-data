package com.entity.log;


import java.io.Serializable;
import java.util.Map;
import lombok.*;

/**
 * 埋点数据实体
 */
@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Event implements Serializable {

    private String project;
    private String distinctId;
    private Long time;
    private String event;
    private Map<String,Object> properties;

}
