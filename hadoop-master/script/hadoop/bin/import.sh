#!/bin/bash

data_path=./data
impala_host=node2

function a {

chown hdfs $data_path/*
chmod +rw $data_path/*

#判断数据文件是否存在并可读
if [ ! -r $data_path/miloancard.del ]
then
	echo Error "$data_path" 中没有要导入的数据，或者数据文件没有读取权限
	exit
fi

#清空并删除数据库
impala-shell -i $impala_host -f dropall.sql
hbase shell drophbase.sql

#建立文件夹并上传数据至HDFS
su hdfs -c "hdfs dfs -rm -R -f /tmp/mydata"
su hdfs -c "hdfs dfs -mkdir -p /tmp/mydata/miloancar"
su hdfs -c "hdfs dfs -mkdir -p /tmp/mydata/gcassuremain"
su hdfs -c "hdfs dfs -mkdir -p /tmp/mydata/gcassuremulticlient"
su hdfs -c "hdfs dfs -mkdir -p /tmp/mydata/employee"
su hdfs -c "hdfs dfs -mkdir -p /tmp/mydata/organization"
su hdfs -c "hdfs dfs -mkdir -p /tmp/mydata/param_class"
su hdfs -c "hdfs dfs -mkdir -p /tmp/mydata/pmparamrelation"
su hdfs -c "hdfs dfs -mkdir -p /tmp/mydata/XCRMS_GROUPRELATION"
su hdfs -c "hdfs dfs -mkdir -p /tmp/mydata/grouptype"

su hdfs -c "hdfs dfs -put $data_path/gcassuremain* /tmp/mydata/gcassuremain/"
su hdfs -c "hdfs dfs -put $data_path/miloancard* /tmp/mydata/miloancar"
su hdfs -c "hdfs dfs -put $data_path/gcassuremulticlient* /tmp/mydata/gcassuremulticlient/"
su hdfs -c "hdfs dfs -put $data_path/employee* /tmp/mydata/employee/"
su hdfs -c "hdfs dfs -put $data_path/organization* /tmp/mydata/organization/"
su hdfs -c "hdfs dfs -put $data_path/param_class* /tmp/mydata/param_class/"
su hdfs -c "hdfs dfs -put $data_path/pmparamrelation* /tmp/mydata/pmparamrelation/"
su hdfs -c "hdfs dfs -put $data_path/XCRMS_GROUPRELATION* /tmp/mydata/XCRMS_GROUPRELATION/"
su hdfs -c "hdfs dfs -put $data_path/grouptype* /tmp/mydata/grouptype/"
su hdfs -c "hadoop jar /mnt/disk0/mydata/huijin-mapreduce.jar cn.huijin.hadoop.mapreduce.PreData /tmp/mydata/miloancar /tmp/mydata/miloancard"

exit
}

#创建表结构并且关联数据
impala-shell -i $impala_host -f create.sql

#预处理数据
impala-shell -i $impala_host -f process.sql

#处理33表查询及统计（网点）
su hdfs -c "hdfs dfs -rm -R /tmp/hbase331"
su hdfs -c "hdfs dfs -rm -R /tmp/hbase332"
impala-shell -i $impala_host -f 33.sql

#处理34表查询及统计（县级）
su hdfs -c "hdfs dfs -rm -R /tmp/hbase341"
su hdfs -c "hdfs dfs -rm -R /tmp/hbase342"
impala-shell -i $impala_host -f 34.sql

#处理35表查询及统计（市级）
su hdfs -c "hdfs dfs -rm -R /tmp/hbase351"
su hdfs -c "hdfs dfs -rm -R /tmp/hbase352"
impala-shell -i $impala_host -f 35.sql

#处理36表查询及统计（省级）
su hdfs -c "hdfs dfs -rm -R /tmp/hbase361"
su hdfs -c "hdfs dfs -rm -R /tmp/hbase362"
impala-shell -i $impala_host -f 36.sql

#导入331至hbase
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,d:busitype,d:custid,d:cname,d:currency,d:contractmoney,d:loantype,d:loanaccount,d:busicode,d:loandate,d:money,d:enddate,d:interest,d:loanpurpose,d:balamt,d:indebtamt,d:outdebtamt,d:accountstate,d:fiveclass,d:tenclass,d:paymode,d:mainassure,d:contractno,d:busimanager,d:assistbusimanager,d:instcitycode,d:deptcode,d:item,d:paydept -Dimporttsv.bulk.output=hdfs://manager:8020/tmp/hbase331/ hbase331 hdfs://manager:8020/user/hive/warehouse/process.db/hbase331/"
su hdfs -c "hdfs dfs -chmod -R 777 /tmp/hbase331"
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://manager:8020/tmp/hbase331/ hbase331"

#导入332至hbase
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,d:cust_num,d:busi_num,d:contractmoney,d:money,d:balAmt,d:payDebt,d:inDebtAmt,d:outDebtAmt -Dimporttsv.bulk.output=hdfs://manager:8020/tmp/hbase332/ hbase332 hdfs://manager:8020/user/hive/warehouse/process.db/hbase332/"
su hdfs -c "hdfs dfs -chmod -R 777 /tmp/hbase332"
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://manager:8020/tmp/hbase332/ hbase332"

#导入341至hbase
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,d:busitype,d:custid,d:cname,d:currency,d:contractmoney,d:loantype,d:loanaccount,d:busicode,d:loandate,d:money,d:enddate,d:interest,d:loanpurpose,d:balamt,d:indebtamt,d:outdebtamt,d:accountstate,d:fiveclass,d:tenclass,d:paymode,d:mainassure,d:contractno,d:busimanager,d:assistbusimanager,d:instcitycode,d:deptcode,d:paydebt,d:item -Dimporttsv.bulk.output=hdfs://manager:8020/tmp/hbase341/ hbase341 hdfs://manager:8020/user/hive/warehouse/process.db/hbase341/"
su hdfs -c "hdfs dfs -chmod -R 777 /tmp/hbase341"
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://manager:8020/tmp/hbase341/ hbase341"

#导入342至hbase
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,d:cust_num,d:busi_num,d:contractmoney,d:money,d:balAmt,d:payDebt,d:inDebtAmt,d:outDebtAmt -Dimporttsv.bulk.output=hdfs://manager:8020/tmp/hbase342/ hbase342 hdfs://manager:8020/user/hive/warehouse/process.db/hbase342/"
su hdfs -c "hdfs dfs -chmod -R 777 /tmp/hbase342"
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://manager:8020/tmp/hbase342/ hbase342"

#导入351至hbase
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,d:busitype,d:custid,d:cname,d:currency,d:contractmoney,d:loantype,d:loanaccount,d:busicode,d:loandate,d:money,d:enddate,d:interest,d:loanpurpose,d:balamt,d:indebtamt,d:outdebtamt,d:accountstate,d:fiveclass,d:tenclass,d:paymode,d:mainassure,d:contractno,d:busimanager,d:assistbusimanager,d:instcitycode,d:deptcode,d:paydebt,d:item -Dimporttsv.bulk.output=hdfs://manager:8020/tmp/hbase351/ hbase351 hdfs://manager:8020/user/hive/warehouse/process.db/hbase351/"
su hdfs -c "hdfs dfs -chmod -R 777 /tmp/hbase351"
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://manager:8020/tmp/hbase351/ hbase351"

#导入352至hbase
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,d:cust_num,d:busi_num,d:contractmoney,d:money,d:balAmt,d:payDebt,d:inDebtAmt,d:outDebtAmt -Dimporttsv.bulk.output=hdfs://manager:8020/tmp/hbase352/ hbase352 hdfs://manager:8020/user/hive/warehouse/process.db/hbase352/"
su hdfs -c "hdfs dfs -chmod -R 777 /tmp/hbase352"
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://manager:8020/tmp/hbase352/ hbase352"

#导入361至hbase
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,d:busitype,d:custid,d:cname,d:currency,d:contractmoney,d:loantype,d:loanaccount,d:busicode,d:loandate,d:money,d:enddate,d:interest,d:loanpurpose,d:balamt,d:indebtamt,d:outdebtamt,d:accountstate,d:fiveclass,d:tenclass,d:paymode,d:mainassure,d:contractno,d:busimanager,d:assistbusimanager,d:instcitycode,d:deptcode,d:paydebt,d:item -Dimporttsv.bulk.output=hdfs://manager:8020/tmp/hbase361/ hbase361 hdfs://manager:8020/user/hive/warehouse/process.db/hbase361/"
su hdfs -c "hdfs dfs -chmod -R 777 /tmp/hbase361"
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://manager:8020/tmp/hbase361/ hbase361"

#导入362至hbase
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,d:cust_num,d:busi_num,d:contractmoney,d:money,d:balAmt,d:payDebt,d:inDebtAmt,d:outDebtAmt -Dimporttsv.bulk.output=hdfs://manager:8020/tmp/hbase362/ hbase362 hdfs://manager:8020/user/hive/warehouse/process.db/hbase362/"
su hdfs -c "hdfs dfs -chmod -R 777 /tmp/hbase362"
su hdfs -c "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://manager:8020/tmp/hbase362/ hbase362"
