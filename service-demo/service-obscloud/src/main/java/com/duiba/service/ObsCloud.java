package com.duiba.service;

import java.io.*;

import com.duiba.util.DateUtils;
import com.duiba.util.Signature;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class ObsCloud {

    private static String accessKey = "IY0A65PZLJ4TMSXYPZ9K"; //取值为获取的AK
    private static String securityKey = "yRvkY2720BRkBj2B5sMTpDIe2u3NrbGkGToqzthU";  //取值为获取的SK
    private static String endPoint = "https://obs.cn-north-4.myhuaweicloud.com";
//    private static String bucketname = "s-bds-oar-push-cs-bj4";
//    private static String bucketname = "obscloud-data";

    public static void main(String[] args) throws ObsException {
//        upload(args[0], args[1], args[2]);
        uploadAccess(args[0], args[1], args[2]);
        close();
    }

    public static ObsClient getInstance() {
        return new ObsClient(accessKey, securityKey, endPoint);
    }

    public static void upload(String bucketname, String objectname, String localfile) throws ObsException {
        ObsClient obsClient = getInstance();
        obsClient.putObject(bucketname, objectname, new File(localfile));
    }

    public static void uploadAccess(String bucketname, String objectname, String localfile) {
        ObsClient obsClient = getInstance();
//        obsClient.setBucketAcl(bucketname, AccessControlList.REST_CANNED_PUBLIC_READ_WRITE_DELIVERED);
//        obsClient.setObjectAcl(bucketname, objectname, AccessControlList.REST_CANNED_PUBLIC_READ_WRITE_DELIVERED);
//        obsClient.putObject(bucketname, objectname, new File(localfile));

        PutObjectRequest request = new PutObjectRequest();
        request.setBucketName(bucketname);
        request.setObjectKey(objectname);
        request.setFile(new File(localfile));
        // 设置对象访问权限为公共读
        request.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
        obsClient.putObject(request);
    }


    public static void list(ObsBucket obsBucket) throws ObsException {
        System.out.println("开始测试桶里面的对象信息");
        ObsClient obsClient = getInstance();
        ObjectListing objList = obsClient.listObjects(obsBucket.getBucketName());
        for (ObsObject obj : objList.getObjects()) {
            System.out.println("--:" + obj.getObjectKey() + " (size=" + obj.getMetadata().getContentLength() + ")");
        }
    }

    public static void close() {
        try {
            ObsClient obsClient = getInstance();
            obsClient.close();
        } catch (IOException e) {
            System.out.println("close obs client error.");
        }

    }


    private static void putObjectToBucket() {
        InputStream inputStream = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse httpResponse = null;
        String requesttime = DateUtils.formateDate(System.currentTimeMillis());
        HttpPut httpPut = new HttpPut("http://bucket001.obs.cn-north-4.myhuaweicloud.com/objecttest1");

        httpPut.addHeader("Date", requesttime);

        /** 根据请求计算签名 **/
        String contentMD5 = "";
        String contentType = "";
        String canonicalizedHeaders = "";
        String canonicalizedResource = "/bucket001/objecttest1";
        // Content-MD5 、Content-Type 没有直接换行， data格式为RFC 1123，和请求中的时间一致
        String canonicalString = "PUT" + "\n" + contentMD5 + "\n" + contentType + "\n" + requesttime + "\n" + canonicalizedHeaders + canonicalizedResource;
        System.out.println("StringToSign:[" + canonicalString + "]");
        String signature = null;
        try {
            signature = Signature.signWithHmacSha1(securityKey, canonicalString);
            // 上传的文件目录
            inputStream = new FileInputStream("D:\\OBSobject\\text01.txt");
            InputStreamEntity entity = new InputStreamEntity(inputStream);
            httpPut.setEntity(entity);

            // 增加签名头域 Authorization: OBS AccessKeyID:signature
            httpPut.addHeader("Authorization", "OBS " + accessKey + ":" + signature);
            httpResponse = httpClient.execute(httpPut);

            // 打印发送请求信息和收到的响应消息
            System.out.println("Request Message:");
            System.out.println(httpPut.getRequestLine());
            for (Header header : httpPut.getAllHeaders()) {
                System.out.println(header.getName() + ":" + header.getValue());
            }

            System.out.println("Response Message:");
            System.out.println(httpResponse.getStatusLine());
            for (Header header : httpResponse.getAllHeaders()) {
                System.out.println(header.getName() + ":" + header.getValue());
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    httpResponse.getEntity().getContent()));

            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = reader.readLine()) != null) {
                response.append(inputLine);
            }
            reader.close();

            // print result
            System.out.println(response.toString());


        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
