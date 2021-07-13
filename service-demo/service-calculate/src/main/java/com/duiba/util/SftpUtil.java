package com.duiba.util;

import com.jcraft.jsch.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.Properties;

public class SftpUtil {
    public ChannelSftp sftp;
    public Session session;

    //连接sftp
    public boolean connect(String path, String addr, int port, String username, String password) throws Exception {
        boolean result = false;
        try {
            JSch jSch = new JSch();
            session = jSch.getSession(username, addr, port);
            if (password != null) {
                session.setPassword(password);
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            Channel channel = session.openChannel("sftp");
            sftp = (ChannelSftp) channel;
            result = true;
        } catch (JSchException e) {
            e.printStackTrace();
            return result;
        }


        if (!isDirExist(path)) {
            String[] tokens = StringUtils.splitPreserveAllTokens(path, "/");
            String newPath = "";
            for (int i = 1; i < tokens.length; i++) {
                try {
                    newPath = newPath + "/" + tokens[i];
                    sftp.cd(newPath);
                } catch (SftpException e) {
                    sftp.mkdir(tokens[i]);
                    sftp.cd(tokens[i]);
                }
                System.out.println(String.valueOf(i) + "======mkdir " + tokens[i]);
            }
            result = isDirExist(path);
        }
        return result;
    }

    //判断路径是否存在
    public boolean isDirExist(String dir) {
        boolean isExist = false;
        try {
            SftpATTRS sftpATTRS = sftp.lstat(dir);
            isExist = true;
            return sftpATTRS.isDir();
        } catch (Exception e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                isExist = false;
            }
        }
        return isExist;
    }

    //hdfs文件上传到sftp
    public void upload(FileSystem fs, Path path, String newName, String outputPath) throws Exception {
        FSDataInputStream fSDataInputStream = fs.open(path, 8020);
        this.sftp.cd(outputPath);
        this.sftp.put((InputStream) fSDataInputStream, newName);
        fSDataInputStream.close();
    }

    //本地文件上传到sftp
    public void uploadcheck(File file, String newName) throws Exception {
        FileInputStream input = new FileInputStream(file);
        this.sftp.put(input, file.getName());
        input.close();
    }

    //关闭连接
    public void close() throws IOException {
        if (sftp != null) {
            if (sftp.isConnected()) {
                sftp.disconnect();
            }
        }
        if (session != null) {
            if (session.isConnected()) {
                session.disconnect();
            }
        }
    }


}
