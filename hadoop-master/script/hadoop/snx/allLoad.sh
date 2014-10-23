#!/bin/bash
#全部增量数据
echo '+++++++++++++++++++上传文件到hdfs+++++++++++++++'
echo '上传miloancard。。。。'
hdfs dfs -mkdir -p /user/hadoop/tmp/miloancard
hdfs dfs -put /data/miloancard/* /user/hadoop/tmp/miloancard
echo 'miloancard上传完成'
echo "已耗费时间：$SECONDS"

echo '上传om_employee。。。。'
hdfs dfs -mkdir -p /user/hadoop/tmp/om_employee
hdfs dfs -put /data/om_employee/* /user/hadoop/tmp/om_employee
echo 'om_employee上传完成'
echo "已耗费时间：$SECONDS"

echo '上传om_organization。。。。'
hdfs dfs -mkdir -p /user/hadoop/tmp/om_organization
hdfs dfs -put /data/om_organization/* /user/hadoop/tmp/om_organization
echo 'om_organization上传完成'
echo "已耗费时间：$SECONDS"

echo '上传XCRMS_GROUPRELATION。。。。'
hdfs dfs -mkdir -p /user/hadoop/tmp/XCRMS_GROUPRELATION
hdfs dfs -put /data/XCRMS_GROUPRELATION/* /user/hadoop/tmp/XCRMS_GROUPRELATION

echo 'XCRMS_GROUPRELATION上传完成'
echo "已耗费时间：$SECONDS"

echo "规整miloancard数据"
hadoop jar /data/report/huijin-mapreduce.jar cn.huijin.hadoop.mapreduce.PreData /user/hadoop/tmp/miloancard /user/hadoop/tmp/miloancard_output

echo "已耗费时间：$SECONDS"

echo "数据导入hive"
hive -e "load data  inpath '/user/hadoop/tmp/miloancard' overwrite into table import.miloancard"
hive -e "load data  inpath '/user/hadoop/tmp/om_organization' overwrite into table import.om_organization"
hive -e "load data  inpath '/user/hadoop/tmp/om_employee' overwrite into table import.om_employee"
hive -e "load data  inpath '/user/hadoop/tmp/XCRMS_GROUPRELATION' overwrite into table import.XCRMS_GROUPRELATION"

echo "更新impala信息"
impala-shell -f ./impala-update.txt

echo "清理临时文件"
hdfs -dfs -rm -r /user/hadoop/tmp/miloancard
hdfs -dfs -rm -r /user/hadoop/tmp/om_employee
hdfs -dfs -rm -r /user/hadoop/tmp/om_organization
hdfs -dfs -rm -r /user/hadoop/tmp/XCRMS_GROUPRELATION

echo "完成"
echo "时间统计：$SECONDS"
