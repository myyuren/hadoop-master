 #!/bin/bash

echo 导出数据到hdfs
time impala-shell --quiet -i node2 -f export071.sql

echo 下载数据文件
time hdfs dfs -getmerge /user/hive/warehouse/process.db/export071 ./export071.data 

echo 删除HDFS中的文件
time impala-shell --quiet -i node2 -q "drop table process.export071"



