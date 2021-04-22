package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseUtil {
    private static Configuration conf;
    private static Logger logger = Logger.getLogger(HBaseUtil.class);

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://datanode:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "datanode:2181");
    }

    /**
     * @param tableName
     * @return
     * @throws IOException
     * @function 判断表是否存在
     */
    public static boolean isExist(String tableName) throws IOException {
        logger.info("正在调用isExist()方法，判断" + tableName + "是否存在");
        //新API
        Connection connection = ConnectionFactory.createConnection(conf);       //工厂设计模式
        Admin admin = connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * @param tableName
     * @param columnFamily
     * @throws Exception
     * @function Hbase表的创建
     */
    public static void createTable(String tableName, String... columnFamily) throws Exception {
        logger.info("正在调用createTable()方法创建表" + tableName);
        Connection connection = ConnectionFactory.createConnection(conf);       //工厂设计模式
        Admin admin = connection.getAdmin();
        //判断表是否存在
        if (isExist(tableName)) {
            throw new Exception("表已存在，不可重复创建");
        } else {
            //创建表描述器
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            for (String family : columnFamily) {
                desc.addFamily(new HColumnDescriptor(family));
            }
            //创建表
            admin.createTable(desc);
            logger.info(tableName + "表成功创建");
        }
    }

    /**
     * @param map 此时用Map集合来封装表名称和列族 HashMap<String,String[]> key为tableName,value为String[]数组的columnFamily
     * @throws Exception
     * @function 批量创建表
     */
    public static void createMultiTables(Map<String, String[]> map) throws Exception {
        logger.info("正在调用createMultiTable()创建表");
        for (Map.Entry<String, String[]> entry : map.entrySet()) {
            //1. 获取tableName和columnFamily
            //1.1 获取tableName
            String tableName = entry.getKey();
            //1.2 获取columnFamily
            String[] columnFamily = entry.getValue();
            //创建表
            createTable(tableName, columnFamily);
        }
    }


    /**
     * @param tableName
     * @throws Exception
     * @function Hbase表的删除
     */
    public static void deleteTable(String tableName) throws Exception {
        logger.info("正在调用deleteTable删除表" + tableName);
        Connection connection = ConnectionFactory.createConnection(conf);       //工厂设计模式
        Admin admin = connection.getAdmin();
        if (isExist(tableName)) {
            if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
                //disable
                admin.disableTable(TableName.valueOf(tableName));
            }
            //删除表
            admin.deleteTable(TableName.valueOf(tableName));
            logger.info(tableName + "表删除成功=======");
        } else {
            throw new Exception(tableName + "表不存在，无法进行删除");
        }
    }

    /**
     * @param list List<String>中封装tableName集合
     * @throws Exception
     * @function 批量删除表
     */
    public static void deleteMultiTables(List<String> list) throws Exception {
        for (String tableName : list) {
            deleteTable(tableName);
        }
    }

    /**
     * @param tableName    表名
     * @param rowKey       行键
     * @param columnFamily 列族
     * @param column       列名
     * @param value        列值
     * @throws IOException
     * @function 向Hbase中添加一行数据
     */
    public static void addRow(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        logger.info("正在调用addRow方法，向" + tableName + "表中添加数据");
        //工厂设计模式
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        //封装待存放的对象rowkey封装
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));      //指定列族，列名，存放的数据
        //添加数据
        table.put(put);     //可以传入Put对象的集合
    }

    /**
     * @function 批量添加数据
     * @param tableName
     * @param puts
     * @throws Exception
     */
    public static void addRows(String tableName, List<Put> puts) throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        HTable htable = (HTable) connection.getTable(TableName.valueOf(tableName));
        htable.setAutoFlushTo(false);
        htable.setWriteBufferSize(5 * 1024 * 1024);
        try {
            htable.put((List<Put>) puts);
            htable.flushCommits();
        } finally {
            if (htable != null)
                htable.close();
            connection.close();
        }
    }


    /**
     * @param tableName
     * @param rowKey
     * @throws IOException
     * @function 删除单行数据
     */
    public static void deleteMultiRow(String tableName, String rowKey) throws IOException {
        logger.info("正在调用deleteMultiRow方法，删除" + tableName + "表中rowKey为" + rowKey + "的数据");
        Connection connection = ConnectionFactory.createConnection(conf);       //工厂设计模式
        Table table = connection.getTable(TableName.valueOf(tableName));        //获取表
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    /**
     * @param tableName
     * @param rowKeys
     * @throws IOException
     * @function 删除多行数据
     */
    public static void deleteMultiRow(String tableName, List<String> rowKeys) throws IOException {
        logger.info("正在调用deleteMultiRow方法，删除" + tableName + "表中" + "多行数据");
        Connection connection = ConnectionFactory.createConnection(conf);       //工厂设计模式
        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        //批量删除
        List<Delete> list = new ArrayList();
        for (String rowKey : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            list.add(delete);
        }
        //批量删除多行
        table.delete(list);                                 //批处理效率更高
    }

    /**
     * @param tableName
     * @throws IOException
     * @function 扫描数据——得到所有数据
     */
    public static void getAllRows(String tableName) throws IOException {
        logger.info("正在调用getAllRows方法，扫描表" + tableName);
        Connection connection = ConnectionFactory.createConnection(conf);       //工厂设计模式
        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();                     //扫描表的配置信息
//      scan.setMaxVersions();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
//          System.out.println(Bytes.toString(result.getRow()));        也可以获取到row key
            Cell[] rawCells = result.rawCells();            //获取所有的cell
            for (Cell cell : rawCells) {
                System.out.println("行健：" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("行健：" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("行健：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("行健：" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("------------------");
            }
        }
    }

    /**
     * @param tableName
     * @param rowKey
     * @param columnFalily
     * @param column
     * @throws IOException
     * @function 得到指定列数据
     */
    public static void getRow(String tableName, String rowKey, String columnFalily, String column) throws IOException {
        logger.info("正在调用getRow方法，获取表" + tableName + "中，rowKey为" + rowKey + "的数据");
        Connection connection = ConnectionFactory.createConnection(conf);       //工厂设计模式
        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFalily), Bytes.toBytes(column));   //设置要获取的列族和列
        Result result = table.get(get);
        Cell[] rawCells = result.rawCells();            //获取所有的cell
        for (Cell cell : rawCells) {
            System.out.println("行健：" + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("行健：" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("行健：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("行健：" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("------------------");
        }
    }
}
