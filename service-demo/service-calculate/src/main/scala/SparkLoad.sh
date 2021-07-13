/opt/app/spark-2.4/bin/spark-submit \
--master k8s://https://10.218.81.170:6443 \
--deploy-mode cluster \
--name ExcelLoad \
--executor-memory 4G \
--executor-cores 2 \
--driver-memory 4g \
--class com.duiba.task.ExcelLoad \
--conf spark.kubernetes.namespace=spark \
--conf spark.executor.instances=4 \
--conf spark.kubernetes.executor.request.cores=0.3 \
--conf spark.kubernetes.executor.limit.cores=2 \
--conf spark.kubernetes.driver.limit.cores=2 \
--conf spark.kubernetes.node.selector.spark=offline \
--conf spark.kubernetes.authenticate.driver.caCertFile=/opt/app/k8s-b-key/ca.crt \
--conf spark.kubernetes.container.image=harbor.dui88.com/library/spark:2.4.3-f \
hdfs://10.50.10.11:8020/user/liuhao2/jar/ExcelLoad.jar \
hdfs://10.50.10.11:8020/user/liuhao2/excel/宁波银行_虚拟社区客户数据4.27.xlsx \
tmp.tmp_demo_demo_test \
1