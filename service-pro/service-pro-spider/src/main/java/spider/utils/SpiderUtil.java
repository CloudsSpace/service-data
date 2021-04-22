package spider.utils;

import org.apache.commons.httpclient.HttpException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class SpiderUtil {

    /**
     * @function 获取html网页源代码
     * @param url
     * @param encoding
     * @return
     */
    public static String getHtml(String url, String encoding) {
        StringBuffer sb = new StringBuffer();
        BufferedReader bfr = null;
        //1.根据网址和网页编码获取网页源代码
        //1.1定义网址和编码
        try {
            //1.2建立连接
            URL objUrl = new URL(url);
            //1.3打开连接
            URLConnection uc = objUrl.openConnection();
            //1.4创建文件输入流 建立管道
            //InputStream isr = uc.getInputStream();
            //1.5建立缓冲流-->创建转换流
            bfr = new BufferedReader(new InputStreamReader(uc.getInputStream(), encoding));
            //1.6通过缓冲流读取网页源代码，一次读取一行
            //内容 !=null bfr.readLine || 字节数 !=-1 bfr.read
            String line = "";
            while ((line = bfr.readLine()) != null) {
                sb.append(line).append("\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bfr != null) {
                try {
                    bfr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return sb.toString();
    }

    /**
     * @function html页面解析
     * @param path
     * @return
     * @throws HttpException
     * @throws IOException
     */
    public  void  analyzePage(String path) throws HttpException, IOException {
        String filename = path.substring(path.lastIndexOf('/') + 1).substring(0, path.substring(path.lastIndexOf('/') + 1).length() - 5);
        // 创建字符输出流类对象和已存在的文件相关联。文件不存在的话，并创建。
        FileWriter writer = new FileWriter(filename + ".txt");
        // 读取网页
        Document doc = Jsoup.connect(path).get();
        String movieTitle = doc.getElementById("movieTitle").text();
        writer.write("movieTitle：" + movieTitle);
        writer.write("\r\n");
        writer.write("***************************\r\n");

        // 获取电影相关信息
        String info = doc.getElementById("info").text();
        writer.write(info);
        writer.write("\r\n");
        writer.write("***************************\r\n");

        // 获取电影评分
        String score = doc.getElementById("rating_num").text();
        writer.write("score：" + score);
        writer.write("\r\n");
        writer.write("***************************\r\n");

        // 获取电影评价
        Elements container = doc.getElementsByClass("article");
        Document containerDoc = Jsoup.parse(container.toString());
        Element comment = containerDoc.getElementById("comments-section");
        Document commentDoc = Jsoup.parse(comment.toString());
        Elements cmtslist = commentDoc.getElementsByClass("comment-item");
        for (Element clearfix : cmtslist) {
            // 获取评论人
            String discussant = clearfix.getElementsByClass("comment-info").text();
            writer.write("discussant：" + discussant);
            writer.write("\r\n");
            // 获取评论时间
            String time = clearfix.getElementsByClass("comment-time").text();
            writer.write("comment-time：" + time);
            writer.write("\r\n");
            // 获取评论内容
            String commentContent = clearfix.select("p").get(0).text();
            writer.write("comment-content：" + commentContent);
            writer.write("\r\n");
            writer.write("====================================\r\n");
        }
        // 刷新该流中的缓冲。将缓冲区中的字符数据保存到目的文件中去。
        writer.flush();
        // 关闭此流
        writer.close();
    }

}
