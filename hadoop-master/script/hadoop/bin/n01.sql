use process;

drop table if exists hbase011;
drop table if exists hbase_custnum012;
drop table if exists hbase_contractmoney012;
drop table if exists hbase_other012;
drop table if exists hbase012;

create table hbase011 as 
select distinct c.* from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS';


---客户总数
create table hbase_custnum012 as 
select concat(d.groupid) as key,count(*) cust_num 
from 
(select distinct o.groupid groupid,c.custid custid from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS') d 
group by d.groupid;

---合同总金额
create table hbase_contractmoney012 as 
select concat(d.groupid) as key, 
sum(
 case
	when d.contractmoney is null 
	then 0 
	else d.contractmoney 
 end) sum_contractmoney 
from 
(select distinct o.groupid groupid,c.contractno contractno,c.contractmoney contractmoney from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS' ) d 
group by d.groupid;

---其他
create table hbase_other012 as 
select concat(o.groupid) as key, COUNT(c.busicode) busi_num,SUM(c.money) money,SUM(c.balAmt) balAmt,
SUM(
        CASE
            WHEN c.paydebt IS NULL
            THEN 0
            ELSE c.paydebt
        END) payDebt, 
SUM(c.inDebtAmt) inDebtAmt, SUM(c.outDebtAmt) outDebtAmt
from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS' group by o.groupid;

---总统计
create table hbase012 row format delimited fields terminated by '\t' stored as textfile as 
select hcn.key key,hcn.cust_num cust_num,ho.busi_num busi_num,hcm.sum_contractmoney contractmoney,ho.money money,ho.balAmt balAmt, 
ho.payDebt payDebt, ho.inDebtAmt inDebtAmt, ho.outDebtAmt outDebtAmt 
from hbase_custnum012 hcn join hbase_contractmoney012 hcm on hcn.key =hcm.key join hbase_other012 ho on hcm.key = ho.key;

drop table hbase_custnum012;
drop table hbase_contractmoney012;
drop table hbase_other012;