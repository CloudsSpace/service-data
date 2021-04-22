package com.entity.log;


import java.util.LinkedHashMap;
import lombok.*;


/**
 * @Author: ysh
 * @Date: 2019/10/28 19:49
 * @Version: 1.0
 */
@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class BinLog {

    private String database;
    private String table;
    private String type;
    private Long ts;
    private Long xid;
    private LinkedHashMap<String, String> data;
    private LinkedHashMap<String, String> old;
    private Boolean commit;

}
