use process;

drop table if exists hbase101;
drop table if exists hbase_custnum102;
drop table if exists hbase_contractmoney102;
drop table if exists hbase_other102;
drop table if exists hbase102;

create table hbase101(
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
) partitioned by(t string, groupid string, paccountstate string) stored as parquet;

insert overwrite hbase101 partition(t="101",groupid,paccountstate)
select distinct c.*, o.groupid, c.accountstate
from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS';


---客户总数
create table hbase_custnum102 as 
select concat(d.groupid,"-", 
case 
when d.accountstate = ''  
then "0" 
else d.accountstate 
end) as key,count(*) cust_num 
from 
(select distinct o.groupid groupid,c.accountstate accountstate,c.custid custid from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS') d 
group by d.groupid,d.accountstate;

---合同总金额
create table hbase_contractmoney102 as 
select concat(d.groupid,"-", 
case 
when d.accountstate = '' 
then "0" 
else d.accountstate 
end) as key, 
sum(
 case
	when d.contractmoney is null 
	then 0 
	else d.contractmoney 
 end) sum_contractmoney 
from 
(select distinct o.groupid groupid,c.accountstate accountstate,c.contractno contractno,c.contractmoney contractmoney from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS' ) d 
group by d.groupid,d.accountstate;

---其他
create table hbase_other102 as 
select concat(o.groupid,"-", 
case 
when c.accountstate = ''  
then "0" 
else c.accountstate 
end) as key, COUNT(c.busicode) busi_num,SUM(c.money) money,SUM(c.balAmt) balAmt,
SUM(
        CASE
            WHEN c.paydebt IS NULL
            THEN 0
            ELSE c.paydebt
        END) payDebt, 
SUM(c.inDebtAmt) inDebtAmt, SUM(c.outDebtAmt) outDebtAmt
from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS' group by o.groupid,c.accountstate;

---总统计
create table hbase102 row format delimited fields terminated by '\t' stored as textfile as 
select hcn.key key,hcn.cust_num cust_num,ho.busi_num busi_num,hcm.sum_contractmoney contractmoney,ho.money money,ho.balAmt balAmt, 
ho.payDebt payDebt, ho.inDebtAmt inDebtAmt, ho.outDebtAmt outDebtAmt 
from hbase_custnum102 hcn join hbase_contractmoney102 hcm on hcn.key =hcm.key join hbase_other102 ho on hcm.key = ho.key;

drop table hbase_custnum102;
drop table hbase_contractmoney102;
drop table hbase_other102;