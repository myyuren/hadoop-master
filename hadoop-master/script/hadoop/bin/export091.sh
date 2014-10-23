 #!bin/bash

echo 导出数据到hdfs
time impala-shell --quiet -i node2 -f export091.sql

echo 下载数据文件
time hdfs dfs -getmerge /user/hive/warehouse/process.db/export091 ./export091.data

echo 删除HDFS中的文件
time impala-shell --quiet -i node2 -q "drop table process.export091"
