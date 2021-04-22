package com.util;


import com.JDBCOperator;
import com.entity.warehouse.DwEntity;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.SparkSession;
import scala.util.matching.Regex;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CombineUtil extends JDBCOperator {
    private Connection conn;

    public CombineUtil() throws SQLException {
        initialize();
        conn = getConnection();
    }

    /**
     * 合并job获取
     *
     * @param dw
     * @param id
     * @return
     * @throws SQLException
     */
    public ArrayList<DwEntity> getInfo(String dw, String id) throws SQLException {
        ArrayList<DwEntity> info = new ArrayList<>();
        String sql;
        if (id != null) {
            sql = "select * from bidb.dw_" + dw + " where status=1 and  combine_state=1  and id in (" + id + ")";
        } else {
            sql = "select * from bidb.dw_" + dw + " where  status=1 and   combine_state=1";
        }
        PreparedStatement statement = conn.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        ArrayList<DwEntity> whDws = putResult(resultSet, DwEntity.class);

        for (DwEntity dwEntity : whDws) {
            info.add(dwEntity);
        }
        statement.close();
        return info;
    }

    /**
     * 数据格式判断
     *
     * @param str
     * @return
     */
    public static boolean isNumeric(String str) {
        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 日期格式判断
     *
     * @param sDate
     * @return
     */
    public static boolean isLegalDate(String sDate) {
        int legalLen = 10;
        if ((sDate == null) || (sDate.length() != legalLen)) {
            return false;
        }
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = formatter.parse(sDate);
            return sDate.equals(formatter.format(date));
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 获取指定目录下数据块个数
     * @param fs
     * @param sparkSession
     * @param srcTable
     * @return
     * @throws Exception
     */
    public static int getHDFSBlocks(FileSystem fs, SparkSession sparkSession, String srcTable) throws Exception {
        int totalFiles = 0;
        int totalBlocks = 0;
        String table = "";
        String db = srcTable.substring(0, 3);
        Matcher tablePattern = Pattern.compile("(?<=\\.).*").matcher(srcTable);
        while (tablePattern.find()) {
            table = tablePattern.group();
        }

        String url = sparkSession.catalog().getDatabase(db).locationUri();
        String combineUrl = url + "/" + table;

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(combineUrl), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            int length = next.getBlockLocations().length;
            totalBlocks += length;
            if (next.getLen() != 0) {
                totalFiles++;
            }
        }
        return totalBlocks;
    }


}
