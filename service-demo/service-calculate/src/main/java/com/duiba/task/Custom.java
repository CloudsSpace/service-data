package com.duiba.task;

import com.duiba.util.PropertiesUtil;
import com.duiba.util.SftpUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Properties;

public class Custom {
   static String src ="hdfs://10.50.10.16:9000/user/liuhao2/demo-202105";
   static String dst ="/upload/demo";
    public static void main(String[] args) throws Exception {
//        Properties properties = PropertiesUtil.getProperties("jdbc.properties");
//        System.out.println(properties.getProperty("jdbc.password"));
        SftpUtil sftpUtil = new SftpUtil();
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://prd-bigdata06:9000");
        FileSystem fs = FileSystem.get(conf);
        sftpUtil.connect(dst,"58.213.97.77",22,"duiba_sftp","gOG2IaSet9v5h#PA");
        sftpUtil.upload(fs,new Path(src),"demo.txt",dst);
        sftpUtil.close();


    }
}
