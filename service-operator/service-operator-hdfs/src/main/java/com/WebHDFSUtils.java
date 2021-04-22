package com;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import net.sf.json.JSONObject;

/**
 * @function webhdfs操作工具类
 */
public class WebHDFSUtils {
    public static String EXCEPTION_WAS_CAUGHT = "";
    public static String EXCEPTION_WAS_CAUGHT１ = "";
    public static String DEFAULT_PROTOCOL = "";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * <b>LISTSTATUS</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS&user.name=hdfs"
     * @param totalDir
     * @return
     * @throws IOException
     */
    public List<String> getHDFSDirs(String totalDir, String host, String port) throws IOException {
        String httpfsUrl =DEFAULT_PROTOCOL + host + ":" + port;
        String spec = MessageFormat.format("/webhdfs/v1{0}?op=LISTSTATUS&user.name={1}", totalDir, "hdfs");
        URL url = new URL(new URL(httpfsUrl), spec);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.connect();
        String resp = result(conn, true);
        conn.disconnect();
       /* JSONObject root = JSON.parseObject(resp);
        int size = root.getJSONObject("FileStatuses").getJSONArray("FileStatus").size();
        List<String> dirs = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            String dir = root.getJSONObject("FileStatuses").getJSONArray("FileStatus").getJSONObject(i).getString("pathSuffix");
            dirs.add(dir);
        }*/
        return null;//dirs;
    }

    /**
     * @function Report the result in STRING way
     * @param conn
     * @param input
     * @return
     * @throws IOException
     */
    public String result(HttpURLConnection conn, boolean input) throws IOException {
        StringBuffer sb = new StringBuffer();
        if (input) {
            InputStream is = conn.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, "utf-8"));
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            reader.close();
            is.close();
        }
        return sb.toString();
    }


    /**
     * @param webhdfs
     * @param stream       the InputStream of file to upload
     * @param hdfsFilePath
     * @param op
     * @param parameters
     * @param method
     * @throws IOException
     */
    public void uploadFile(String webhdfs, InputStream stream, String hdfsFilePath, String op, Map<String, String> parameters, String method) throws IOException {
        HttpURLConnection con;
        try {
            con = getConnection(webhdfs, hdfsFilePath, op, parameters, method);

            byte[] bytes = new byte[1024];
            int rc = 0;
            while ((rc = stream.read(bytes, 0, bytes.length)) > 0) con.getOutputStream().write(bytes, 0, rc);
            con.getInputStream();
            con.disconnect();
        } catch (IOException e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        }
        stream.close();
    }

    /**
     * @param webhdfs
     * @param hdfsFilePath
     * @param op
     * @param parameters
     * @param method
     * @throws IOException
     */
    public Map<String, Object> getFileStatus(String[] webhdfs, String hdfsFilePath, String op, Map<String, String> parameters, String method) {
        Map<String, Object> fileStatus = new HashMap<String, Object>();
        HttpURLConnection connection = null;
        for (String url : webhdfs) {
            try {
                HttpURLConnection conn = getConnection(url, hdfsFilePath, op, parameters, method);
                if (conn.getInputStream() != null) {
                    connection = conn;
                    break;
                }
            } catch (IOException e) {
                logger.error("");
            }
        }
        StringBuffer sb = new StringBuffer();
        try {
            InputStream is = connection.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            reader.close();
            System.out.println(sb.toString());
           /* JSONObject root = JSONObject.fromObject(sb.toString());
            JSONObject status = root.getJSONObject("FileStatus");
            Iterator keys = status.keys();
            while (keys.hasNext()) {
                String key = keys.next().toString();
                String value = status.get(key).toString();
                fileStatus.put(key, value);
            } *///            is.close();
        } catch (IOException e) {
            logger.error(EXCEPTION_WAS_CAUGHT, e);
        } catch (NullPointerException e) {
            logger.error(EXCEPTION_WAS_CAUGHT, e);
        }
        return fileStatus;
    }

    /**
     * @param strurl     webhdfs like http://ip:port/webhdfs/v1 ,port usually 50070 or 14000
     * @param path       hdfs path + hdfs filename  eg:/user/razor/readme.txt
     * @param op         the operation for hdfsFile eg:GETFILESTATUS,OPEN,MKDIRS,CREATE etc.
     * @param parameters other parameter if you need
     * @param method     method eg: GET POST PUT etc.
     * @return
     */
    public HttpURLConnection getConnection(String strurl, String path, String op, Map<String, String> parameters, String method) {
        URL url = null;
        HttpURLConnection con = null;
        StringBuffer sb = new StringBuffer();
        try {
            sb.append(strurl);
            sb.append(path);
            sb.append("?op=");
            sb.append(op);
            if (parameters != null) {
                for (String key : parameters.keySet()) sb.append("&").append(key + "=" + parameters.get(key));
            }
            url = new URL(sb.toString());
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod(method);
            con.setRequestProperty("accept", "*/*");
            con.setRequestProperty("connection", "Keep-Alive");
            String s = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)";
            String s1 = "ozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)";
            con.setRequestProperty("User-Agent", s1);
//            con.setRequestProperty("Accept-Encoding", "gzip");
//            con.setDoInput(true);
            con.setDoOutput(true);
            con.setUseCaches(false);
        } catch (IOException e) {
            logger.error(EXCEPTION_WAS_CAUGHT, e);
        }
        return con;
    }
}