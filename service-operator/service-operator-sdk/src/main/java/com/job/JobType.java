package com.job;

/**
 * @Classname JobType
 * @Description TODO
 * @Date 2020/2/10 17:13
 * @Created by ysh
 */
public enum JobType {

    SPARK("spark"),

    FLINK("flink"),

    KAFKA("kafka"),

    REDIS("redis"),

    ES("es"),

    HDFS("hdfs"),

    HBASE("hbase"),

    JDBC("jdbc");

    private String type;

    JobType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
