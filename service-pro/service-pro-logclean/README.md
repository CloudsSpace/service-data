# **spark日志清洗**
source:日志数据
业务数据：切分数据

业务逻辑
-------------------------------------------------------
1.读取数据时进行过滤，保留状态为“[ERROR]”的数据。

2.对每行数据进行切割，去除“[ERROR]”字段后，得到时间戳、批次、以及json格式的数据

3.采用正则对每行的json数据进行过滤，筛掉json格式有误的数据

4.采用FastJson处理json格式数据，对普通json字段直接调用JSONObject进行解析，json数组调用JSONArray进行处理后调用JSONObject进行解析

5.对拿到的字段进行拼接，需要的字段有：

    timestr(string) ,offsetNum(int),app_user_id(string) ,vid(string) ,time(string) ,fromtype (string) ,url (string) ,referrer_url (string),event (string) ,type (string) ,app_id (string) ,channel_id (string) ,device_id (string) ,extra_id (string)    

 程序运行需要添加输入、输出路径：     
---------------------------------------------------------
    args(0):hdfs://jianbing/dwh/ods/log/trace/hf-lib-other-logconsume.log.${df.format(new Date())}
    args(1):hdfs://jianbing/dwh/dw/dwd/log/trace/输出文件名

    bin/spark-submit --master spark://jianbing:7077 --class com.jianbing.details.LogDetail /d2/local/data/dwd/LogDetail-jar-with-dependencies.jar hdfs://jianbing/dwh/ods/log/trace/hf-lib-other-logconsume.log hdfs://jianbing/dwh/dw/dwd/log/trace
注：${df.format(new Date())}为当前时间

azkanban任务调度
--------------------------------------------------------
文件1：clean_hbase_hive_delete.flow

    nodes:
      - name: clean_data
        type: command
        com.config:
          command: sh clean_data.sh
    
      - name: load_hbase1
        type: command
        com.config:
          command: sh load_hbase1.sh
        dependsOn:
          - clean_data
    
      - name: load_hbase2
        type: command
        com.config:
          command: sh load_hbase2.sh
        dependsOn:
          - load_hbase1
    
      - name: load_hive
        type: command
        com.config:
          command: sh load_hive.sh
        dependsOn:
          - load_hbase2
    
      - name: delete_path
        type: command
        com.config:
          command: sh delete_path.sh
        dependsOn:
          - load_hive
文件2：flow20.project
       
       azkaban-flow-version: 2.0               

clean_data.sh：

    #!/bin/sh
    source /etc/profile && spark-submit --master yarn --deploy-mode cluster --class com.jianbing.detail.LogDetail hdfs://jianbing/jars/LogDetail-jar-with-dependencies.jar hdfs://jianbing/dwh/ods/log/trace/cat-end-logconsume.log.$(date -d '-1 day' +'%Y-%m-%d') hdfs://jianbing/dwh/dw/dwd/log/trace

load_hbase1.sh：

    #!/bin/sh
    source /etc/profile && spark-submit --master yarn --deploy-mode cluster --class com.jianbing.main.LogAnalysis LogAnalysis-jar-with-dependencies.jar

load_hbase2.sh：

    #!/bin/sh
    source /etc/profile && spark-submit --master yarn --deploy-mode cluster --class com.jianbing.main.ExtraIdCount ExtraIdCount-jar-with-dependencies.jar

load_hive.sh：

    #!/bin/sh
    source /etc/profile && hive -e "LOAD DATA INPATH 'hdfs://jianbing/dwh/dw/dwd/log/trace/part*' INTO TABLE dwd.log_trace;"

delete_path：

    #!/bin/sh
    source /etc/profile && hdfs dfs -rm -r hdfs://jianbing/dwh/dw/dwd/log/trace
                 
注：hive分流，extra不解析，hbase分流中解析

老数据（ios、android）:user_id与app_user_id作用相同，user_id就是app_user_id。数据中有app_user_id的就直接取，数据中没有app_user_id的需要将user_id与AppUsers数据关联后取出app_user_id，
新数据（h5）:user_id与app_user_id作用不同，user_id为平台用户id。新数据以app_user_id取值为主，直接取，并且数据中只有app_user_id。
                
字段顺序：            
hive分流:val arr = Array("app_user_id", "vid", "time", "fromtype", "url", "referrer_url", "event", "type", "app_id", "channel_id", "device_id","extra")
hbase分流：val arr = Array("app_user_id", "vid", "time", "url", "referrer_url", "event", "type", "app_id", "channel_id", "device_id","extra", "fromtype")
              
              
              
              


