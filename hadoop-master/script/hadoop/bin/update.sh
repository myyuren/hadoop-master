 
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,d:busitype,d:custid,d:cname,d:currency,d:contractmoney,d:loantype,d:loanaccount,d:busicode,d:loandate,d:money,d:enddate,d:interest,d:loanpurpose,d:balamt,d:indebtamt,d:outdebtamt,d:accountstate,d:fiveclass,d:tenclass,d:paymode,d:mainassure,d:contractno,d:busimanager,d:assistbusimanager,d:instcitycode,d:deptcode -Dimporttsv.bulk.output=hdfs://manager:8020/tmp/hbase011/ hbase011 hdfs://manager:8020/user/hive/warehouse/out.db/mlc_main_client_hbase/

hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://manager:8020/tmp/hbase011/ hbase011

