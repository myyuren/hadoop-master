use process;

drop table if exists hbase041;
drop table if exists hbase_custnum042;
drop table if exists hbase_contractmoney042;
drop table if exists hbase_other042;
drop table if exists hbase042;

create table hbase041(
 busitype			string,
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
 deptcode		    string, 
 paydebt			double, 
 item				string, 
 limittype			string, 
 custtype			string, 
 floatrate			double, 
 industry			string  
) partitioned by(t string, citycode string, pdeptcode string) stored as parquet;

insert overwrite hbase041 partition(t="041",citycode,pdeptcode)
select *, instcitycode, deptCode  
from mlc_main_client;


---客户总数
create table hbase_custnum042 as 
select concat(c.instCityCode,"-",c.deptCode) as key,count(*) cust_num 
from 
(select distinct instCityCode,deptCode,custid from mlc_main_client) c 
group by c.instCityCode,c.deptCode;

---合同总金额
create table hbase_contractmoney042 as 
select concat(c.instCityCode,"-",c.deptCode) as key, 
sum(
 case
	when c.contractmoney is null 
	then 0 
	else c.contractmoney 
 end) sum_contractmoney 
from 
(select distinct instCityCode,deptCode,contractno,contractmoney from mlc_main_client ) c 
group by c.instCityCode,c.deptCode;

---其他
create table hbase_other042 as 
select concat(c.instCityCode,"-",c.deptCode) as key, COUNT(c.busicode) busi_num,SUM(c.money) money,SUM(c.balAmt) balAmt,
SUM(
        CASE
            WHEN c.paydebt IS NULL
            THEN 0
            ELSE c.paydebt
        END) payDebt, 
SUM(c.inDebtAmt) inDebtAmt, SUM(c.outDebtAmt) outDebtAmt
from mlc_main_client c group by c.instCityCode,c.deptCode;

---总统计
create table hbase042 row format delimited fields terminated by '\t' stored as textfile as 
select hcn.key key,hcn.cust_num cust_num,ho.busi_num busi_num,hcm.sum_contractmoney contractmoney,ho.money money,ho.balAmt balAmt, 
ho.payDebt payDebt, ho.inDebtAmt inDebtAmt, ho.outDebtAmt outDebtAmt 
from hbase_custnum042 hcn join hbase_contractmoney042 hcm on hcn.key =hcm.key join hbase_other042 ho on hcm.key = ho.key;

drop table hbase_custnum042;
drop table hbase_contractmoney042;
drop table hbase_other042;