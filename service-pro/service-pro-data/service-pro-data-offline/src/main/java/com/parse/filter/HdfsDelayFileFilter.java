package com.parse.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.Serializable;


/**
 *
 * 扫描HDFS文件时过滤指定文件
 *
 * @Author: ysh
 * @Date: 2019/5/31 14:25
 * @Version: 1.0
 */
public class HdfsDelayFileFilter implements Serializable, PathFilter {

    /**
     * 过滤事件数据文件名为"-1"的文件
     *
     * @param path
     * @return
     */
    @Override
    public boolean accept(Path path){
        String tmpStr =  path.getName();
        if (tmpStr.equals("-1")) {
            return true;
        }
        return false;
    }
}
