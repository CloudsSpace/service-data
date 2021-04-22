package com;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class SocketProducer {
    public static void main(String[] args) throws InterruptedException {
        try {
            //1.建立客户端socket连接，指定服务器位置及端口
            ServerSocket server = new ServerSocket(2222);
            Socket socket =server.accept();
            //2.得到socket读写流
            OutputStream os = socket.getOutputStream();
            PrintWriter pw = new PrintWriter(os);
            //输入流
            InputStream is = socket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream("D:\\admin\\Desktop\\log"));
            int tempchar;
            while ((tempchar = inputStreamReader.read()) != -1) {
                //屏蔽掉换行符
                if (((char) tempchar) != '\r') {
                    pw.write((char) tempchar);
                    pw.flush();
                }
                Thread.sleep(1);
            }
            //3.利用流按照一定的操作，对socket进行读写操作
            //socket.shutdownOutput();
            //接收服务器的相应
            String reply = null;
            while (!((reply = br.readLine()) == null)) {
                System.out.println("接收服务器的信息：" + reply);
            }
            //4.关闭资源
            inputStreamReader.close();
            br.close();
            is.close();
            pw.close();
            os.close();
            socket.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
