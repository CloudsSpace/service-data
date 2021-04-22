package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HdfsUtil {
    static Configuration conf = new Configuration();
    private static String webhdfs = "webhdfs://gaiaa:50070";
    //conf.set("fs.default.name", "hdfs://localhost:9000");

    /**
     * @param conf
     * @param path
     * @return
     * @throws IOException
     * @function 判断文件是否存在
     */
    public static boolean test(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(path));
    }

    /**
     * @param conf
     * @param remoteFilePath
     * @throws IOException
     * @function 查看文件内容
     */
    public static void cat(Configuration conf, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        FSDataInputStream in = fs.open(remotePath);
        BufferedReader d = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line = d.readLine()) != null) {
            System.out.println(line);
        }
        d.close();
        in.close();
        fs.close();
    }

    /**
     * @function 追加文本内容，文件不存在就创建，文件存在则删除
     * @param content
     * @param remoteFilePath
     * @throws IOException
     */
    public static void appendToFile(String content, String remoteFilePath) throws IOException {
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        try {
            //判断文件是否存在
            if ( !test(conf, remoteFilePath) ) {
                System.out.println("文件不存在: " + remoteFilePath);
                //创建新文件
                touchz(conf, remoteFilePath);
                System.out.println("已创建文件: " + remoteFilePath);
                //写入内容
                appendContent(conf, content, remoteFilePath);
                System.out.println("文件已写入: " + remoteFilePath);
            } else {
                appendContent(conf, content, remoteFilePath);
                System.out.println("文件已写入: " + remoteFilePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * @param content
     * @param remoteFilePath
     * @throws IOException
     * @function 追加文本内容
     */
    public static void appendContent(Configuration conf,String content, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        //创建一个文件输出流，输出的内容将追加到文件末尾
        FSDataOutputStream out = fs.append(remotePath);
        out.write(content.getBytes());
        out.close();
        fs.close();
    }


    /**
     * @param localUrl
     * @param hdfsUrl
     * @function 追加文件内容
     */
    public static void AppendFile(String localUrl, String hdfsUrl) {
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        String inpath = localUrl;
        String hdfs_path = hdfsUrl;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(hdfs_path), conf);
            InputStream in = new BufferedInputStream(new FileInputStream(inpath));
            OutputStream out = fs.append(new Path(hdfs_path));
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param conf
     * @param remoteFilePath
     * @param localFilePath
     * @throws IOException
     * @function 移动文件到本地移动文件到本地，移动后，删除源文件
     */
    public static void moveToLocalFile(Configuration conf, String remoteFilePath, String localFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        Path localPath = new Path(localFilePath);
        fs.moveToLocalFile(remotePath, localPath);
    }

    /**
     * @param conf
     * @param localFilePath
     * @param remoteFilePath
     * @throws IOException
     * @function 复制文件到指定路径, 若路径已存在，则进行覆盖
     */
    public static void copyFromLocalFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        //fs.copyFromLocalFile 第一个参数表示是否删除源文件，第二个参数表示是否覆盖 */
        fs.copyFromLocalFile(false, true, localPath, remotePath);
        fs.close();
    }

    /**
     * @param conf
     * @param remoteDir
     * @return true: 空，false: 非空
     * @throws IOException
     * @function 判断目录是否为空
     */
    public static boolean isDirEmpty(Configuration conf, String remoteDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(remoteDir);
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(dirPath, true);
        return !remoteIterator.hasNext();
    }

    /**
     * @param conf
     * @param remoteDir
     * @return
     * @throws IOException
     * @function 创建目录
     */
    public static boolean mkdir(Configuration conf, String remoteDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(remoteDir);
        boolean result = fs.mkdirs(dirPath);
        fs.close();
        return result;
    }

    /**
     * @param conf
     * @param remoteDir
     * @return
     * @throws IOException
     * @function 删除目录
     */
    public static boolean rmDir(Configuration conf, String remoteDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(remoteDir);
        /* 第二个参数表示是否递归删除所有文件 */
        boolean result = fs.delete(dirPath, true);
        fs.close();
        return result;
    }

    /**
     * @param conf
     * @param remoteFilePath
     * @throws IOException
     * @function 创建文件
     */
    public static void touchz(Configuration conf, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        FSDataOutputStream outputStream = fs.create(remotePath);
        outputStream.close();
        fs.close();
    }

    /**
     * @param conf
     * @param remoteFilePath
     * @return
     * @throws IOException
     * @function 删除文件
     */
    public static boolean rm(Configuration conf, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        boolean result = fs.delete(remotePath, false);
        fs.close();
        return result;
    }

    /**
     * @param conf
     * @param remoteFilePath
     * @param remoteToFilePath
     * @return
     * @throws IOException
     * @function 移动文件
     */
    public static boolean mv(Configuration conf, String remoteFilePath, String remoteToFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(remoteFilePath);
        Path dstPath = new Path(remoteToFilePath);
        boolean result = fs.rename(srcPath, dstPath);
        fs.close();
        return result;
    }

    /**
     * @param localFName
     * @param hdfsFName
     * @throws IOException
     * @function 上传文件
     */
    public static void uploadFileFromLocal(String localFName, String hdfsFName) throws IOException {
        // 云端HDFS文件路径 user/hadoop
        String hdfsURI = webhdfs + hdfsFName;
        InputStream in = new BufferedInputStream(new FileInputStream(localFName));
        FileSystem fs = FileSystem.get(URI.create(hdfsURI), conf); // 创建文件系统 对象
        OutputStream out = fs.create(new Path(hdfsURI), new Progressable() {
            // 输出流
            public void progress() {
                System.out.println("上传完成一个文件到HDFS");
            }
        });
        IOUtils.copyBytes(in, out, 1024, true);
        // 连接两个流，形成通道，使输入流向输出流传输数据
        in.close();
        fs.close();
        out.close();
    }

    /**
     * @param localFName
     * @param hdfsFName
     * @throws FileNotFoundException
     * @throws IOException
     * @function 下载文件
     */
    private static void downFileFromHDFS(String localFName, String hdfsFName) throws
            FileNotFoundException, IOException {
        String hdfsFPath = webhdfs + hdfsFName;
        Configuration conf = new Configuration();
        // 获取conf配置
        FileSystem fs = FileSystem.get(URI.create(hdfsFPath), conf); // 创建文件系统对象
        FSDataInputStream outHDFS = fs.open(new Path(hdfsFPath)); // 从HDFS读出文件流
        OutputStream inLocal = new FileOutputStream(localFName); // 写入本地文件
        IOUtils.copyBytes(outHDFS, inLocal, 1024, true);
        fs.close();
        outHDFS.close();
        inLocal.close();
    }


    /**
     * @throws
     * @Title: createFilePath
     * @param：@param filePath 文件夹名字路径（"/EvenDir"、"/EvenDir/temp"）
     * @return：boolean 创建成功返回true，失败返回false
     * @Description：TODO 创建文件夹
     * @author linqinghong
     * @date 2019 年4月03日下午11:53:39
     */
    public static boolean createFilePath(String filePath) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path dfs = new Path(filePath);
            return fs.mkdirs(dfs);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    /**
     * @throws
     * @Title: createFile
     * @param：@param fileName 文件路径及文件名
     * @param：@param message 文件内容
     * @return：void
     * @Description：TODO 创建文件
     * @author linqinghong
     * @date 2019 年4月03日下午11:53:39
     */
    public static void createFile(String fileName, String message) {
        FileSystem fs = null;
        FSDataOutputStream out = null;
        try {
            fs = FileSystem.get(conf);
            Path path = new Path(fileName);
            out = fs.create(path);
            out.writeUTF(message);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                out.flush();
                out.close();
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @throws
     * @Title: deleteFile
     * @param：@param filePath 文件、文件夹路径
     * @return：boolean 删除成功返回true，失败返回false
     * @Description：TODO 删除文件（删除文件夹与删除文件相同，若文件夹内有文件需要递归删除）
     * @author linqinghong
     * @date 2019 年4月03日下午11:53:39
     */
    public static boolean deleteFile(String filePath) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path fileName = new Path(filePath);
            return fs.delete(fileName, true);
            // 若path为文件或空目录则忽略true，否则为true时删除文件夹下所有
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    /**
     * @throws
     * @Title:renameFile
     * @param：@param oldName 旧名字
     * @param：@param newName 新名字
     * @return：boolean 重命名返回true，失败返回false
     * @Description：TODO 文件（文件夹）重命名
     * @author linqinghong
     * @date 2019 年4月03日下午11:53:39
     */
    public static boolean renameFile(String oldName, String newName) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path oldPath = new Path(oldName);
            Path newPath = new Path(newName);
            return fs.rename(oldPath, newPath);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    /**
     * @throws
     * @Title: fileExist
     * @param：@param filePath 文件（文件夹）路径
     * @return：boolean 存在返回true，不存在返回false
     * @Description：TODO 判断文件或文件夹是否存在
     * @author linqinghong
     * @date 2019 年4月03日下午11:53:39
     */
    public static boolean fileExist(String filePath) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path fileName = new Path(filePath);
            return fs.exists(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    /**
     * @Title: fileUpdateTime
     * @param：@param filePath 文件路径及文件名
     * @return：void
     * @Description：TODO 查看文件大小及修改时间
     * @author linqinghong
     * @date 2019 年4月03日下午11:53:39
     */
    public static void fileUpdateTime(String filePath) {
        FileSystem fs = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            fs = FileSystem.get(conf);
            Path fileName = new Path(filePath);
            FileStatus status = fs.getFileStatus(fileName);
            System.out.println("文件大小：" + status.getLen());
            System.out.println("文件修改时间：" + sdf.format(new Date(status.getModificationTime())));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @throws
     * @Title: fileList
     * @param：@param filePath 文件夹路径
     * @return：void
     * @Description： TODO 查看文件夹下文件名和文件路径
     */
    public static void fileList(String filePath) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path fileName = new Path(filePath);
            FileStatus[] status = fs.listStatus(fileName);
            Path[] path = FileUtil.stat2Paths(status);
            for (Path file : path) {
                System.out.println("文件名:" + file.getName());
                System.out.println("文件路径:" + file);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        // System.out.println(createFilePath("/EvenDir/oHno"));
        // System.out.println(deleteFile("/EvenDir/no"));
        // System.out.println(renameFile("/EvenDir/yes", "/EvenDir/no"));
        // System.out.println(fileExist("/EvenDir/temp/test11"));
        // fileList("/EvenDir");
        //fileUpdateTime(webhdfs+"/data/tmp/ods_import_call_record.txt");
        // createFile("/EvenDir/yes/text.txt", "even good!!");
        // downLoadFile("/EvenDir/yes/", "text.txt", "E:/");
       /* String sql = "SELECT count(1) as count FROM CRF_P2P_APP_FILE_INFO";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = DBManager.getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            if (rs.next()) {
                System.out.println(rs.getString("count"));
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
    }

    /**
     * @throws
     * @function 得到当前已上传记录的编号
     * @param：@return
     * @return：int
     */
    public static Integer getNumber() {
        BufferedReader reader = null;
        Integer num = null;
        String str = null;
        try {
            reader = new BufferedReader(new FileReader(new File("src/CurruntNum.txt")));
            str = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (str != null && !"".equals(str.trim())) {
            num = Integer.parseInt(str);
        }
        return num;
    }

    /**
     * @throws
     * @function 设置已上传编号
     * @return：void
     */
    public static void setNumber(String num) {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(new File("src/CurruntNum.txt")));
            writer.write(num);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
