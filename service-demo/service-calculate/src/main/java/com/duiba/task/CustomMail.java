package com.duiba.task;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Authenticator;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;

import com.sun.mail.util.MailSSLSocketFactory;
import org.apache.commons.lang3.StringUtils;

public class CustomMail {
    private String host = "smtp.qq.com";

    public static void main(String[] args) {
//        String receive = "576955037@qq.com";
//        String subject = "邮件主题";
//        String msg = "邮件内容";
//        String filename = "/Users/admin/Desktop/202105/白名单.txt";
//        try {
//            sendMail(receive, subject, msg, filename);
//        } catch (GeneralSecurityException e) {
//            e.printStackTrace();
//        }
        sendEmail("liuhao.mail@foxmail.com","576955037@qq.com","mhnlhyojllsebajb");
    }

    /**
     * 发送邮件 包含附件
     *
     * @param toEmail
     * @param fromEmail
     * @param password
     */
    public static void sendEmail(String toEmail, String fromEmail, String password) {
        try {
            Properties props = new Properties();
            String host = "smtp.sea-level.com.cn";
            props.put("mail.smtp.host", "smtp.qq.com"); // SMTP主机名
            props.put("mail.smtp.auth", "true"); // 是否需要用户认证
            props.put("mail.smtp.port", "587"); // 主机端口号
            props.put("mail.smtp.starttls.enable", "true"); // 启用TLS加密
            props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");

            // 获取Session实例:
            Session session = Session.getInstance(props, new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(fromEmail, password);
                }
            });
            MimeMessage message = new MimeMessage(session);
            // 设置发送方地址:
            message.setFrom(new InternetAddress(fromEmail));
            // 设置接收方地址:
            message.setRecipient(Message.RecipientType.TO, new InternetAddress(toEmail));
            // 设置邮件主题:
            message.setSubject("Hello", "UTF-8");
            // 设置邮件正文:这是含有附件的邮件
            Multipart multipart = new MimeMultipart();
            // 添加text:
            BodyPart textpart = new MimeBodyPart();
            String body = "这是一个带有附件的邮件，请注意查收附件";
            textpart.setContent(body, "text/html;charset=utf-8");
            multipart.addBodyPart(textpart);
            // 添加附件: 直接发送内容
           /* BodyPart filepart = new MimeBodyPart();
            String fileName ="附件.txt";
            filepart.setFileName(fileName);
            //input 在附件中写入内容
            String input ="这里居然是文件的内容";
            try {
                filepart.setDataHandler(new DataHandler(new ByteArrayDataSource(input, "application/octet-stream")));
            } catch (IOException e) {
                e.printStackTrace();
            }
            multipart.addBodyPart(filepart);*/
            //发送邮件，发送本地文件
            BodyPart filepart = new MimeBodyPart();
            String fileName = "附件.txt";
            filepart.setFileName(fileName);
            //path 要发送的本地文件的路径
            String path = "/Users/admin/Desktop/202105/白名单.txt";
            FileDataSource fileDataSource = new FileDataSource(new File(path));
            filepart.setDataHandler(new DataHandler(fileDataSource));
            multipart.addBodyPart(filepart);
            // 设置邮件内容为multipart:
            message.setContent(multipart);
            // 发送:
            Transport.send(message);
        } catch (MessagingException e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送带附件的邮件
     *
     * @param receive  收件人
     * @param subject  邮件主题
     * @param msg      邮件内容
     * @param filename 附件地址
     * @return
     * @throws GeneralSecurityException
     */
    public static boolean sendMail(String receive, String subject, String msg, String filename)
            throws GeneralSecurityException {
        if (StringUtils.isEmpty(receive)) {
            return false;
        }

        // 发件人电子邮箱
        final String from = "123456789@163.com";
        // 发件人电子邮箱密码
        final String pass = "123456";

        // 指定发送邮件的主机为 smtp.qq.com
        String host = "smtp.163.com"; // 邮件服务器

        // 获取系统属性
        Properties properties = System.getProperties();

        // 设置邮件服务器
        properties.setProperty("mail.smtp.host", host);

        properties.put("mail.smtp.auth", "true");
        MailSSLSocketFactory sf = new MailSSLSocketFactory();
        sf.setTrustAllHosts(true);
        properties.put("mail.smtp.ssl.enable", "true");
        properties.put("mail.smtp.ssl.socketFactory", sf);
        // 获取默认session对象
        Session session = Session.getDefaultInstance(properties, new Authenticator() {
            public PasswordAuthentication getPasswordAuthentication() { // qq邮箱服务器账户、第三方登录授权码
                return new PasswordAuthentication(from, pass); // 发件人邮件用户名、密码
            }
        });

        try {
            // 创建默认的 MimeMessage 对象
            MimeMessage message = new MimeMessage(session);

            // Set From: 头部头字段
            message.setFrom(new InternetAddress(from));

            // Set To: 头部头字段
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(receive));

            // Set Subject: 主题文字
            message.setSubject(subject);

            // 创建消息部分
            BodyPart messageBodyPart = new MimeBodyPart();

            // 消息
            messageBodyPart.setText(msg);

            // 创建多重消息
            Multipart multipart = new MimeMultipart();

            // 设置文本消息部分
            multipart.addBodyPart(messageBodyPart);

            // 附件部分
            messageBodyPart = new MimeBodyPart();
            // 设置要发送附件的文件路径
            DataSource source = new FileDataSource(filename);
            messageBodyPart.setDataHandler(new DataHandler(source));

            // messageBodyPart.setFileName(filename);
            // 处理附件名称中文（附带文件路径）乱码问题
            messageBodyPart.setFileName(MimeUtility.encodeText(filename));
            multipart.addBodyPart(messageBodyPart);

            // 发送完整消息
            message.setContent(multipart);

            // 发送消息
            Transport.send(message);
            // System.out.println("Sent message successfully....");
            return true;
        } catch (MessagingException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return false;
    }
}
