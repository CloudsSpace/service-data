package spider.mains;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;

public class DataTest {
    public static void main(String[] args) throws IOException {
        String url = "https://movie.douban.com/subject/4739952/comments?status=P";
//        String encoding = "utf-8";
//        String html = SpiderUtil.getHtml(url, encoding);
//        System.out.println(html);
        // String url  = "https://movie.douban.com/subject/4739952/comments?status=P";
        //String url = "https://www.open-open.com/jsoup/dom-navigation.htm";
        Document document = Jsoup.connect(url).get();
        String title = document.getElementsByTag("title").text();
        String comment = document.getElementsByClass("comment").text();
        document.body();
        document.text();

    }
}
