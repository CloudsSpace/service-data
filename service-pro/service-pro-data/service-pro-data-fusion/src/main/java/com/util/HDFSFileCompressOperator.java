package com.util;

import com.sdk.HDFSOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @Author: ysh
 * @Date: 2019/10/21 16:32
 * @Version: 1.0
 */
public class HDFSFileCompressOperator extends HDFSOperator {

    private static Path eventPath = new Path("/user/eventData/events");
    private static FileSystem fs;
    private static CompressionCodec codec;
    private static Configuration cf;

    public HDFSFileCompressOperator() {
        initialize();
    }

    @Override
    public void initialize() {
        fs = initFileSystem();
        codec = createCompressionCodec("org.apache.hadoop.io.compress.GzipCodec");
        cf = getConfiguration();
    }

    @Override
    public void close() {
        try {
            if (fs != null) {
                fs.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除合并压缩前的小时文件
     *
     * @param path
     * @throws IOException
     */
    public static void deleteNoGzipFile(Path path) throws IOException {
        ArrayList<Path> files = new ArrayList<>();
        Boolean delete = false;
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (int i = 0; i < fileStatuses.length; i++) {
            String fileName = fileStatuses[i].getPath().getName();
            if (!fileName.endsWith(".gz")) {
                files.add(fileStatuses[i].getPath());
            } else {
                delete = true;
            }
        }
        int count = 0;
        if (delete) {
            for (int i = 0; i < files.size(); i++) {
                boolean del = fs.delete(files.get(i));
                if (!del) {
                    System.out.println("删除" + files.get(i) + " 失败");
                }
                count++;
            }
        }
        System.out.println("删除" + path + " " + count + "个文件");
    }


    /**
     * 获取指定日期所有事件路径
     *
     * @param staticdate
     * @return
     */
    public LinkedList<Path> getEventList(String staticdate) {
        LinkedHashMap<String, String> tablePathList = new LinkedHashMap<String, String>();
        LinkedList<Path> eventList = new LinkedList<>();
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
                if (!path.contains("_temporary") && !path.contains("_SUCCESS") && fs.exists(new Path(path)) && !name.equals("production_others") && !name.equals("default_others")) {
                    //获取表名
                    tablePathList.put(path, name);
                    long length = fs.getContentSummary(new Path(path)).getLength();
                    while (treeMap.containsKey(length)) {
                        length++;
                    }
                    treeMap.put(length, path);
                }
            }


            for (Long key : treeMap.keySet()) {
                eventList.add(new Path(treeMap.get(key)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return eventList;
    }

    /**
     * 压缩合并后的文件
     *
     * @param path
     * @throws IOException
     */
    public static void compress(String path) throws IOException {
        FSDataOutputStream out = fs.create(new Path(path + ".gz"));
        CompressionOutputStream outputStream = codec.createOutputStream(out);
        FSDataInputStream in = fs.open(new Path(path));
        IOUtils.copyBytes(in, outputStream, cf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(outputStream);
    }

    /**
     * 解压文件
     *
     * @param
     * @throws Exception
     */
    public static void uncompress(String path) throws IOException {
        FSDataInputStream inputStream = fs.open(new Path(path));
        InputStream in = codec.createInputStream(inputStream);
        FSDataOutputStream out = fs.create(new Path(path.replace(".gz", "")));
        IOUtils.copyBytes(in, out, cf);
        IOUtils.closeStream(in);
    }


}
