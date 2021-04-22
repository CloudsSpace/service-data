package com;

import java.io.*;

public class Salary {
    public static void main(String[] args) throws IOException {
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream("D:\\admin\\Desktop\\demo.txt"));
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream("D:\\admin\\Desktop\\output.txt"));
        int stream = 0;
        while ((stream = inputStream.read()) != -1) {
        outputStream.write(stream);
        }

        inputStream.close();
        outputStream.close();
    }


}
