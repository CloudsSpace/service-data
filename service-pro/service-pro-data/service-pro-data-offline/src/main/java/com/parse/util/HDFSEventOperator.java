package com.parse.util;

import com.sdk.HDFSOperator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.collection.mutable.LinkedHashMap;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;


/**
 * 操作HDFS工具类
 * 1.获取事件指定日期路径集合
 * 2.获取和创建事件 Schema
 *
 * @Author: ysh
 * @Date: 2019/5/31 14:30
 * @Version: 1.0
 */
public class HDFSEventOperator extends HDFSOperator {

    private static Path eventPath = new Path("/user/eventData/events");
    private static FileSystem fs;

    public HDFSEventOperator() {
        initialize();
    }

    public boolean exists(String path) throws IOException {
        Path path1 = new Path(path);
        if (!fs.exists(path1)) {
            return false;
        }
        FileStatus[] fileStatuses = fs.listStatus(path1);
        if (fileStatuses.length != 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void initialize() {
        fs = initFileSystem();
    }

    @Override
    public void close() {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public LinkedHashMap<String, String> getEventPathList(String staticdate) {
        LinkedHashMap<String, String> tablePathList = new LinkedHashMap<String, String>();
        LinkedHashMap<String, String> sortMap = new LinkedHashMap<String, String>();
        //根据目录大小进行排序
        TreeMap<Long, String> treeMap = new TreeMap<Long, String>(new Comparator() {
            public int compare(Object o1, Object o2) {
                //如果有空值，直接返回0
                if (o1 == null || o2 == null)
                    return 0;
                return Long.valueOf(o2.toString()).compareTo(Long.valueOf(o1.toString()));
            }
        });
        try {
            FileStatus[] fileStatuses = fs.listStatus(eventPath);
            for (int i = 0; i < fileStatuses.length; i++) {
                String name = fileStatuses[i].getPath().getName();
                String path = fileStatuses[i].getPath() + "/" + staticdate;
                if (!path.contains("_temporary") && !path.contains("_SUCCESS") && fs.exists(new Path(path))) {
                    //获取表名
                    tablePathList.put(path, name);
                    treeMap.put(fs.getContentSummary(new Path(path)).getLength(), path);
                }
            }
            for (Long key : treeMap.keySet()) {
                String path = treeMap.get(key);
                sortMap.put(path, tablePathList.get(path).get());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sortMap;
    }


}
