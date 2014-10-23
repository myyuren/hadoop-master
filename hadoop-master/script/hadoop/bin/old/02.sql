use process;

create table hbase021 row format delimited fields terminated by '\t' stored as textfile as 
select 
concat(c.instCityCode,"-",o.groupid,"-",lpad(cast(floor(round(c.CONTRACTMONEY, 2)*100) as string),11,"0"),"-",c.LOANDATE,"-",lpad(cast(floor(round(c.BALAMT, 2)*100) as string),11,"0"),"-",lpad(cast(99999999999-floor(round(c.MONEY, 2)*100) as string),11,"0"),"-",c.busicode) as key , c.* 
from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='DSHZ';

---�ͻ�����
create table hbase_custnum022 as 
select concat(d.instCityCode,"-",d.groupid) as key,count(*) cust_num 
from 
(select distinct c.instCityCode instCityCode,o.groupid groupid,c.custid custid from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='DSHZ') d 
group by d.instCityCode,d.groupid;

---��ͬ�ܽ��
create table hbase_contractmoney022 as 
select concat(d.instCityCode,"-",d.groupid) as key, 
sum(
 case
	when d.contractmoney is null 
	then 0 
	else d.contractmoney 
 end) sum_contractmoney 
from 
(select distinct c.instCityCode instCityCode,o.groupid groupid,c.contractno contractno,c.contractmoney contractmoney from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='DSHZ' ) d 
group by d.instCityCode,d.groupid;

---����
create table hbase_other022 as 
select concat(c.instCityCode,"-",o.groupid) as key, COUNT(c.busicode) busi_num,SUM(c.money) money,SUM(c.balAmt) balAmt,
SUM(
        CASE
            WHEN c.paydebt IS NULL
            THEN 0
            ELSE c.paydebt
        END) payDebt, 
SUM(c.inDebtAmt) inDebtAmt, SUM(c.outDebtAmt) outDebtAmt
from mlc_main_client c join xcrms_grouprelation r on c.deptcode = r.lowgroupid join
om_organization o on r.upgroupid = o.groupid join grouptype gt on o.groupid =  gt.groupid and gt.orgtype ='DSHZ' group by c.instCityCode,o.groupid;

---��ͳ��
create table hbase022 row format delimited fields terminated by '\t' stored as textfile as 
select hcn.key key,hcn.cust_num cust_num,ho.busi_num busi_num,hcm.sum_contractmoney contractmoney,ho.money money,ho.balAmt balAmt, 
ho.payDebt payDebt, ho.inDebtAmt inDebtAmt, ho.outDebtAmt outDebtAmt 
from hbase_custnum022 hcn join hbase_contractmoney022 hcm on hcn.key =hcm.key join hbase_other022 ho on hcm.key = ho.key;

drop table hbase_custnum022;
drop table hbase_contractmoney022;
drop table hbase_other022;