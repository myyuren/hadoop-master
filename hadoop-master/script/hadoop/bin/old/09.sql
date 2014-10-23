use process;

create table hbase091 row format delimited fields terminated by '\t' stored as textfile as 
select 
concat(o.groupid,"-",lpad(cast(floor(round(c.CONTRACTMONEY, 2)*100) as string),11,"0"),"-",c.LOANDATE,"-",lpad(cast(floor(round(c.BALAMT, 2)*100) as string),11,"0"),"-",lpad(cast(99999999999-floor(round(c.MONEY, 2)*100) as string),11,"0"),"-",c.busicode) as key , c.* 
from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS';

---客户总数
create table hbase_custnum092 as 
select concat(d.groupid,"-", 
case 
when d.fiveclass = ''  
then "0" 
else d.fiveclass 
end) as key,count(*) cust_num 
from 
(select distinct o.groupid groupid,c.fiveclass fiveclass,c.custid custid from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS') d 
group by d.groupid,d.fiveclass;

---合同总金额
create table hbase_contractmoney092 as 
select concat(d.groupid,"-", 
case 
when d.fiveclass = '' 
then "0" 
else d.fiveclass 
end) as key, 
sum(
 case
	when d.contractmoney is null 
	then 0 
	else d.contractmoney 
 end) sum_contractmoney 
from 
(select distinct o.groupid groupid,c.fiveclass fiveclass,c.contractno contractno,c.contractmoney contractmoney from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS' ) d 
group by d.groupid,d.fiveclass;

---其他
create table hbase_other092 as 
select concat(o.groupid,"-", 
case 
when c.fiveclass = ''  
then "0" 
else c.fiveclass 
end) as key, COUNT(c.busicode) busi_num,SUM(c.money) money,SUM(c.balAmt) balAmt,
SUM(
        CASE
            WHEN c.paydebt IS NULL
            THEN 0
            ELSE c.paydebt
        END) payDebt, 
SUM(c.inDebtAmt) inDebtAmt, SUM(c.outDebtAmt) outDebtAmt
from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='SLS' group by o.groupid,c.fiveclass;

---总统计
create table hbase092 row format delimited fields terminated by '\t' stored as textfile as 
select hcn.key key,hcn.cust_num cust_num,ho.busi_num busi_num,hcm.sum_contractmoney contractmoney,ho.money money,ho.balAmt balAmt, 
ho.payDebt payDebt, ho.inDebtAmt inDebtAmt, ho.outDebtAmt outDebtAmt 
from hbase_custnum092 hcn join hbase_contractmoney092 hcm on hcn.key =hcm.key join hbase_other092 ho on hcm.key = ho.key;

drop table hbase_custnum092;
drop table hbase_contractmoney092;
drop table hbase_other092;