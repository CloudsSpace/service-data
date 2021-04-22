package com;


import com.util.HDFSFileCompressOperator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * 定期对原始数据压缩
 * 1.对根据事件和日期对文件进行合并
 * 2.压缩合并后的文件
 * 3.删除非压缩文件
 *
 * @Author: ysh
 * @Date: 2019/5/31 14:25
 * @Version: 1.0
 */
public class DataCompress {

    private static final Logger logger = LoggerFactory.getLogger(DataCompress.class);

    public static void main(String[] args) {
        //准备需要压缩的文件路径
        ExecutorService compressThreadPool = Executors.newFixedThreadPool(10);
        final String staticDate;
        if (args != null) {
            staticDate = args[0];
        } else {
            return;
        }

        HDFSFileCompressOperator hdfsFileCompressOperator = new HDFSFileCompressOperator();
        LinkedList<Path> fileList = hdfsFileCompressOperator.getEventList(staticDate);
        final CountDownLatch countDownLatch = new CountDownLatch(fileList.size());

        //提交压缩任务
        logger.info("start compress: " + staticDate);
        for (final Path path : fileList) {
            compressThreadPool.submit(new Runnable() {
                public void run() {
                    try {
                        String fileName = path.getParent().getName() + "_" + staticDate;
                        logger.info("开始压缩 " + fileName);
                        mergeFile(path, fileName);
                        HDFSFileCompressOperator.compress(path + "/" + fileName);
                        HDFSFileCompressOperator.deleteNoGzipFile(path);
                        logger.info("压缩完成 " + fileName);
                    } catch (IOException e) {
                        logger.info("压缩失败: " + path + "msg " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        synchronized (countDownLatch) {
                            countDownLatch.countDown();
                        }
                    }
                }
            });
        }
        //释放资源
        try {
            countDownLatch.await();
            compressThreadPool.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("success compress: " + staticDate);

    }


    /**
     * 合并小时文件
     *
     * @param path
     * @param fileName
     */
    private static void mergeFile(Path path, String fileName) {
        String cmd = null;
        try {
            cmd = ("hdfs dfs -cat " + path + "/* | hdfs dfs -put - " + path + "/" + fileName).replace("$", "\\$");
            String[] cmds = {"/bin/sh", "-c", cmd};
            Process ps = Runtime.getRuntime().exec(cmds);
            ps.waitFor();
        } catch (Exception e) {
            System.out.println("合并 " + cmd + "失败");
            e.printStackTrace();
        }
    }

}
