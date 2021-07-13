package com.duiba.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;

public class ExecUtil {
    /**
     * 调用方法
     *
     * @param cmd cmd命令
     * @return
     */
    public static String exec(String cmd) {
        try {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(cmd);
            InputStream es = proc.getErrorStream();
            String line;
            BufferedReader br;
            br = new BufferedReader(new InputStreamReader(es, "GBK"));
            StringBuffer buffer = new StringBuffer();
            while ((line = br.readLine()) != null) {
                buffer.append(line + "\n");
            }
            return buffer.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 运行shell脚本
     *
     * @param shell 需要运行的shell脚本
     */
    public static void execShell(String shell) {
        try {
            Runtime.getRuntime().exec(shell);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 运行shell脚本 new String[]方式
     *
     * @param shell 需要运行的shell脚本
     */
    public static void execShellBin(String shell) {
        try {
            Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", shell}, null, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 运行shell并获得结果，注意：如果sh中含有awk,一定要按new String[]{"/bin/sh","-c",shStr}写,才可以获得流
     *
     * @param shStr 需要执行的shell
     * @return
     */
    public static List<String> runShell(String shStr) {
        List<String> strList = new ArrayList<String>();
        try {
            Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", shStr}, null, null);
            InputStreamReader ir = new InputStreamReader(process.getInputStream());
            LineNumberReader input = new LineNumberReader(ir);
            String line;
            process.waitFor();
            while ((line = input.readLine()) != null) {
                strList.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return strList;
    }

    public static void main(String[] args) {
        String shell = "sh /Users/admin/workspace/idea/service-data/service-demo/service-calculate/src/main/scala/demo.sh";
        String test ="ls";
        List<String> data = runShell(shell);
        System.out.println(data);
    }

}
