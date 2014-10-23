use process;

create table hbase041 row format delimited fields terminated by '\t' stored as textfile as 
select 
concat(instCityCode,"-",deptCode,"-",lpad(cast(floor(round(CONTRACTMONEY, 2)*100) as string),11,"0"),"-",LOANDATE,"-",lpad(cast(floor(round(BALAMT, 2)*100) as string),11,"0"),"-",lpad(cast(99999999999-floor(round(MONEY, 2)*100) as string),11,"0"),"-",busicode) as key , * 
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