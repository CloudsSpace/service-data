package com.duiba.util;

import scala.util.matching.Regex;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlUtil {

    private static String filepath = "/Users/admin/workspace/idea/service-data/service-demo/service-calculate/data/";
    private static String db = "ods_credits";
    private static String table = "duiba_tb_live_agent_card";
    private static String comment = "直播代理人名片";
    private static String updatekey = "cur_data";
    private static String keywords = "`cur_data`";
    //    private static String keywords = "`live_id`,`dpm`";
//    private static String updatekey = "live_id,dpm";
    private static String fieldstr = "";

    public static void main(String[] args) throws IOException {

//        SqlUtil.mysqlCreates();
        SqlUtil.hiveCreats();
    }

    /**
     * mysql建表语句及sqoop脚本
     *
     * @return
     * @throws IOException
     */
    public static String mysqlCreates() throws IOException {
        FileReader fileReader = new FileReader(filepath + "hive-fields.csv");
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String str = null;
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder sqoopString = new StringBuilder();

        stringBuilder.append("CREATE TABLE `" + table + "` (\n" +
                "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n");

        while ((str = bufferedReader.readLine()) != null) {
            String[] split = str.split("\t");
            switch (split[2]) {
                case "bigint":
                    stringBuilder.append("  `" + split[1] + "` bigint(20) DEFAULT NULL COMMENT '" + split[3] + "',\n");
                    fieldstr += "" + split[1] + ",";
                    break;
                case "string":
                    stringBuilder.append("  `" + split[1] + "`varchar(64) DEFAULT NULL COMMENT '" + split[3] + "',\n");
                    fieldstr += "" + split[1] + ",";
                    break;
                case "double":
                    stringBuilder.append("  `" + split[1] + "`double DEFAULT NULL COMMENT '" + split[3] + "',\n");
                    fieldstr += "" + split[1] + ",";
                case "decimal(10,4)":
                    stringBuilder.append("  `" + split[1] + "`decimal(10,4) DEFAULT NULL COMMENT '" + split[3] + "',\n");
                    fieldstr += "" + split[1] + ",";

            }
        }
        stringBuilder.append("  `gmt_create` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
                "  `gmt_modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `uk_ad` (" + keywords + "),\n" +
                "  KEY `idx_gmt` (`gmt_create`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=94 DEFAULT CHARSET=utf8mb4 COMMENT='" + comment + "';\n");

        System.out.println(stringBuilder);
        System.out.println(SqlUtil.sqoopExport());
        fileReader.close();
        bufferedReader.close();
        return stringBuilder.toString();
    }

    /**
     * hive comment
     *
     * @return
     * @throws IOException
     */
    public static String hiveCreats() throws IOException {
        FileReader fileReader = new FileReader(filepath + "mysql-fields.sql");
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        StringBuilder stringBuilder = new StringBuilder();
        String str = null;
        int num = 0;
        Pattern compile = Pattern.compile("`.*`.* COMMENT '.*'");
        while ((str = bufferedReader.readLine()) != null) {
//            System.out.println(str);
            Matcher group = compile.matcher(str);
            while (group.find()) {
                String[] arr = group.group().split(" ");
                String comment = group.group().split("COMMENT ")[1];
                if (arr[1].contains("bigint")) {
                    stringBuilder.append("ALTER TABLE " + table + " CHANGE COLUMN " + arr[0] + " " + arr[0] + " bigint COMMENT " + comment + ";\n");
                } else if (arr[1].contains("float")) {
                    stringBuilder.append("ALTER TABLE " + table + " CHANGE COLUMN " + arr[0] + " " + arr[0] + " float COMMENT " + comment + ";\n");
                } else if (arr[1].contains("double")) {
                    stringBuilder.append("ALTER TABLE " + table + " CHANGE COLUMN " + arr[0] + " " + arr[0] + " double COMMENT " + comment + ";\n");
                } else {
                    stringBuilder.append("ALTER TABLE " + table + " CHANGE COLUMN " + arr[0] + " " + arr[0] + " string COMMENT " + comment + ";\n");
                }
            }

        }
        System.out.println(SqlUtil.sqoopImport() + "\n");
        System.out.println(stringBuilder.toString());
        fileReader.close();
        bufferedReader.close();
        return "";
    }

    /**
     * sqoop导出
     *
     * @return
     */
    public static String sqoopExport() {
        String str = "sqoop export \\\n" +
                "--connect ${secret.credits_statistics.jdbc} \\\n" +
                "--username ${secret.credits_statistics.account} \\\n" +
                "--password ${secret.credits_statistics.password} \\\n" +
                "--table \"" + table + "\" \\\n" +
                "--columns \"" + fieldstr + "\" \\\n" +
                "--export-dir /user/hive/warehouse/" + db + ".db/" + table + "/dt=${date} \\\n" +
                "--input-fields-terminated-by \"\\001\"  \\\n" +
                "--input-null-string '\\\\N' \\\n" +
                "--input-null-non-string '\\\\N' \\\n" +
                "--update-key " + updatekey + "\\\n" +
                "--update-mode allowinsert -- \\\n" +
                "--default-character-set=utf-8;";
        return str;
    }

    /**
     * sqoop导入
     *
     * @return
     */
    public static String sqoopImport() {
        String str = "sqoop import --connect ${secret.rds.duiba_live_activity.jdbc} \\\n" +
                "--username ${secret.rds.duiba_live.account} \\\n" +
                "--password ${secret.rds.duiba_live.password} \\\n" +
                "--query \"select * from " + table + " where  \\$CONDITIONS\" \\\n" +
                "--split-by id \\\n" +
                "--hive-table " + db + "." + table + " \\\n" +
                "--target-dir /user/hive/warehouse/temp_query_result.db/sqoop_" + table + " \\\n" +
                "--hive-partition-key dt \\\n" +
                "--hive-partition-value ${date} \\\n" +
                "--hive-import \\\n" +
                "--null-string '\\\\N' \\\n" +
                "--null-non-string '\\\\N' \\\n" +
                "--driver com.mysql.jdbc.Driver \\\n" +
                "--boundary-query \"SELECT MIN(id), MAX(id) FROM " + table + " \" \\\n" +
                "--hive-drop-import-delims \\\n" +
                "--hive-overwrite;";
        return str;
    }

    /**
     * 字段解析
     *
     * @return
     * @throws IOException
     */
    public static String fieldParse() throws IOException {
        String path = "/Users/admin/workspace/idea/service-data/service-demo/service-calculate/data/hologress.csv";
        FileReader fileReader = new FileReader(path);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String str = null;
        int num = 0;
        StringBuilder stringBuilder = new StringBuilder();
        while ((str = bufferedReader.readLine()) != null) {

            if (str.contains("bigint")) {
                stringBuilder.append("{\\\"index\\\":" + num + ",\\\"type\\\":\\\"long\\\"},\n");
            } else if (str.contains("varchar")) {
                stringBuilder.append("{\\\"index\\\":" + num + ",\\\"type\\\":\\\"string\\\"},\n");
            }
            num++;
        }
        System.out.println(stringBuilder.toString());
        fileReader.close();
        bufferedReader.close();
        return stringBuilder.toString();
    }


    public static String mysqlCreate() throws IOException {
        FileReader fileReader = new FileReader(filepath + "hive-fields.sql");
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String str = null;
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder sqoopString = new StringBuilder();
        Pattern compile = Pattern.compile("  .*");
        stringBuilder.append("CREATE TABLE `" + table + "` (\n" +
                "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n");
        while ((str = bufferedReader.readLine()) != null) {
            Matcher matcher = compile.matcher(str);
            while (matcher.find()) {
                String group = matcher.group();
                if (group.contains("bigint")) {
                    stringBuilder.append(group.replaceAll("bigint", "bigint(20) DEFAULT 0") + "\n");
                } else if (group.contains("double")) {
                    stringBuilder.append(group.replaceAll("double", "double DEFAULT 0.0") + "\n");
                } else if (group.contains("decimal")) {
                    stringBuilder.append(group.replaceAll("double", "double DEFAULT 0.0") + "\n");
                } else {
                    stringBuilder.append(group.replaceAll("string", "varchar(64) DEFAULT NULL ") + "\n");
                }
            }
            Matcher field = Pattern.compile("(?<=    ).*?(?=bigint|double|float|string)").matcher(str);
            while (field.find()) {
                fieldstr += "" + field.group() + ",";
            }

        }

        stringBuilder.append("  `gmt_create` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
                "  `gmt_modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `uk_ad` (" + keywords + "),\n" +
                "  KEY `idx_gmt` (`gmt_create`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=94 DEFAULT CHARSET=utf8mb4 COMMENT='" + comment + "';\n");


        System.out.println(stringBuilder);
        System.out.println(SqlUtil.sqoopExport());
        fileReader.close();
        bufferedReader.close();
        return stringBuilder.toString();
    }


}
