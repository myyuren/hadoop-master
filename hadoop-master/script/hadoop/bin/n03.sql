use process;

drop table if exists hbase031;
drop table if exists hbase_custnum032;
drop table if exists hbase_contractmoney032;
drop table if exists hbase_other032;
drop table if exists hbase032;

create table hbase031(
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
) partitioned by(t string, citycode string, groupid string) stored as parquet;

insert overwrite hbase031 partition(t="031",citycode,groupid)
select distinct c.*, c.instcitycode, o.groupid 
from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='XLS';

---客户总数
create table hbase_custnum032 as 
select concat(d.instCityCode,"-",d.groupid) as key,count(*) cust_num 
from 
(select distinct c.instCityCode instCityCode,o.groupid groupid,c.custid custid from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='XLS') d 
group by d.instCityCode,d.groupid;

---合同总金额
create table hbase_contractmoney032 as 
select concat(d.instCityCode,"-",d.groupid) as key, 
sum(
 case
	when d.contractmoney is null 
	then 0 
	else d.contractmoney 
 end) sum_contractmoney 
from 
(select distinct c.instCityCode instCityCode,o.groupid groupid,c.contractno contractno,c.contractmoney contractmoney from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='XLS' ) d 
group by d.instCityCode,d.groupid;

---其他
create table hbase_other032 as 
select concat(c.instCityCode,"-",o.groupid) as key, COUNT(c.busicode) busi_num,SUM(c.money) money,SUM(c.balAmt) balAmt,
SUM(
        CASE
            WHEN c.paydebt IS NULL
            THEN 0
            ELSE c.paydebt
        END) payDebt, 
SUM(c.inDebtAmt) inDebtAmt, SUM(c.outDebtAmt) outDebtAmt
from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='XLS' group by c.instCityCode,o.groupid;

---总统计
create table hbase032 row format delimited fields terminated by '\t' stored as textfile as 
select hcn.key key,hcn.cust_num cust_num,ho.busi_num busi_num,hcm.sum_contractmoney contractmoney,ho.money money,ho.balAmt balAmt, 
ho.payDebt payDebt, ho.inDebtAmt inDebtAmt, ho.outDebtAmt outDebtAmt 
from hbase_custnum032 hcn join hbase_contractmoney032 hcm on hcn.key =hcm.key join hbase_other032 ho on hcm.key = ho.key; 

drop table hbase_custnum032;
drop table hbase_contractmoney032;
drop table hbase_other032;