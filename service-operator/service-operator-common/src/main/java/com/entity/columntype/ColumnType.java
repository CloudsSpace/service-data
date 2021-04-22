package com.entity.columntype;

/**
 * @Classname ColumnType
 * @Description TODO
 * @Date 2020/2/28 12:11
 * @Created by ysh
 */
public enum  ColumnType {

    STRING(1,"String"),

    LONG(2,"Long"),

    BOOLEAN(3,"Boolean"),

    DOUBLE(4,"Double");

    private int type;
    private String typeName;

    ColumnType(int type, String typeName) {
        this.type = type;
        this.typeName = typeName;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }
}
