package com.entity.warehouse;

import lombok.*;

@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class DwEntity {

    private long id;
    private long project;
    private String source;
    private String sink;
    private String ddl;
    private String describe;
    private String execSqls;
    private long execCycle;
    private long execLevel;
    private long external;
    private String externalName;
    private String partitioned;


}
