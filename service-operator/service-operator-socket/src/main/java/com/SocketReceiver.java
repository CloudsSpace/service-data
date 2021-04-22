package com;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class SocketReceiver {
    public static void main(String[] args) throws IOException {
        // 创建socket
        Socket s = new Socket("localhost", 5555);
        // 获得输入流
        java.io.InputStream in = s.getInputStream();
        //读取字节流
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        in.close();
        s.close();

    }
}
