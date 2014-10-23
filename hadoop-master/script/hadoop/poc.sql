--创建表结构

--加载表数据

--将三个表join成一个mlc_main_client
create table mlc_main_client stored as parquet as
SELECT distinct
    busitype,
    mlc.custid,
    mlc.cname,
    mlc.currency,
    mlc.contractmoney,
    mlc.loantype,
    mlc.loanaccount,
    mlc.busicode,
    mlc.loandate,
    mlc.money,
    mlc.enddate,
    mlc.interest,
    mlc.loanpurpose,
    mlc.balamt,
    mlc.indebtamt,
    mlc.outdebtamt,
    mlc.accountstate,
    mlc.fiveclass,
    mlc.tenclass,
    mlc.paymode,
    mlc.mainassure,
    mlc.contractno,
    mlc.busimanager,
    mlc.assistbusimanager,
    mlc.instcitycode,
    mlc.deptcode
from miloancard mlc 
join GCASSUREMAIN main on mlc.CONTRACTNO = main.UPKEYCODE join GCASSUREMULTICLIENT client on client.keycode = main.keycode 
where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816'


--------------------------------------------------------------------------------------1.1
create table mlc_main_client_ordered stored as parquet as
SELECT * from mlc_main_client order by CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 15000000;


--------------------------------------------------------------------------------------
create table mlc_main_client_partitionedby_city(
 busitype           string,
 custid             string,
 cname              string,
 currency           string,
 contractmoney      double,
 loantype           string,
 loanaccount        string,
 busicode           string,
 loandate           string,
 money              double,
 enddate            string,
 interest           double,
 loanpurpose        string,
 balamt             double,
 indebtamt          double,
 outdebtamt         double,
 accountstate       string,
 fiveclass          string,
 tenclass           string,
 paymode            string,
 mainassure         string,
 contractno         string,
 busimanager        string,
 assistbusimanager  string,
 deptcode           string
) partitioned by(t string, citycode string) stored as parquet;

--------------------------------------------------------------------------------------
insert overwrite mlc_main_client_partitionedby_city partition(t='city', citycode)
SELECT
    busitype,
    mlc.custid,
    mlc.cname,
    mlc.currency,
    mlc.contractmoney,
    mlc.loantype,
    mlc.loanaccount,
    mlc.busicode,
    mlc.loandate,
    mlc.money,
    mlc.enddate,
    mlc.interest,
    mlc.loanpurpose,
    mlc.balamt,
    mlc.indebtamt,
    mlc.outdebtamt,
    mlc.accountstate,
    mlc.fiveclass,
    mlc.tenclass,
    mlc.paymode,
    mlc.mainassure,
    mlc.contractno,
    mlc.busimanager,
    mlc.assistbusimanager,
    mlc.deptcode,
    mlc.instcitycode
from mlc_main_client_ordered mlc 
order by CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 15000000;

--------------------------------------------------------------------------------------3.1
create table mlc_main_client_partitionedby_county(
 busitype           string,
 custid             string,
 cname              string,
 currency           string,
 contractmoney      double,
 loantype           string,
 loanaccount        string,
 busicode           string,
 loandate           string,
 money              double,
 enddate            string,
 interest           double,
 loanpurpose        string,
 balamt             double,
 indebtamt          double,
 outdebtamt         double,
 accountstate       string,
 fiveclass          string,
 tenclass           string,
 paymode            string,
 mainassure         string,
 contractno         string,
 busimanager        string,
 assistbusimanager  string,
 instcitycode       string,
 deptcode           string
) partitioned by(t string, county string) stored as parquet;

--------------------------------------------------------------------------------------

create table relation as
SELECT r.upgroupid, r.lowgroupid FROM XCRMS_GROUPRELATION r join om_organization o on r.upgroupid = o.groupid WHERE o.orglevel = 3;

insert overwrite mlc_main_client_partitionedby_county partition(t='county', county)
SELECT
    busitype,
    mlc.custid,
    mlc.cname,
    mlc.currency,
    mlc.contractmoney,
    mlc.loantype,
    mlc.loanaccount,
    mlc.busicode,
    mlc.loandate,
    mlc.money,
    mlc.enddate,
    mlc.interest,
    mlc.loanpurpose,
    mlc.balamt,
    mlc.indebtamt,
    mlc.outdebtamt,
    mlc.accountstate,
    mlc.fiveclass,
    mlc.tenclass,
    mlc.paymode,
    mlc.mainassure,
    mlc.contractno,
    mlc.busimanager,
    mlc.assistbusimanager,
    mlc.deptcode,
    mlc.instcitycode,
    r.upgroupid
from mlc_main_client_ordered mlc join relation r on r.lowgroupid = mlc.deptcode
order by CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 15000000;


CREATE TABLE mlc_main_client_hbase
 row format delimited fields terminated by '\t'
 stored as textfile
as select concat(
lpad(cast(contractmoney as string), 9, '0'), '-', loandate, '-', 
lpad(cast(BALAMT as string), 9, '0'), '-',
lpad(cast(1000000000-money as string), 9, '0')
) key, * from mlc_main_client;

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,d:busitype,d:custid,d:cname,d:currency,d:contractmoney,d:loantype,d:loanaccount,d:busicode,d:loandate,d:money,d:enddate,d:interest,d:loanpurpose,d:balamt,d:indebtamt,d:outdebtamt,d:accountstate,d:fiveclass,d:tenclass,d:paymode,d:mainassure,d:contractno,d:busimanager,d:assistbusimanager,d:instcitycode,d:deptcode -Dimporttsv.bulk.output=hdfs://manager:8020/tmp/hbase011/ hbase011 hdfs://manager:8020/user/hive/warehouse/out.db/mlc_main_client_hbase/

hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://manager:8020/tmp/hbase011/ hbase011

