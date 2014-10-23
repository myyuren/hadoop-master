use process;

drop table if exists hbase071;
drop table if exists hbase_custnum072;
drop table if exists hbase_contractmoney072;
drop table if exists hbase_other072;
drop table if exists hbase072;

create table hbase071(
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
 deptcode		    string, 
 paydebt            double, 
 item               string, 
 limittype          string, 
 custtype           string, 
 floatrate          double, 
 industry           string,
 upparamcode        string 
) stored as parquet;

insert overwrite hbase071
select distinct c.*,p.upparamcode
from hbase011 c join pmParamRelation p on p.lowParamCode = c.loanPurpose and p.paramClassId = '00257';


---客户总数
create table hbase_custnum072 as 
select concat( 
case 
when d.upParamCode = ''  
then "0" 
else d.upParamCode 
end) as key,count(*) cust_num 
from 
(select distinct upParamCode upParamCode,custid custid from hbase071) d 
group by d.upParamCode;

---合同总金额
create table hbase_contractmoney072 as 
select concat( 
case 
when d.upParamCode = '' 
then "0" 
else d.upParamCode 
end) as key, 
sum(
 case
	when d.contractmoney is null 
	then 0 
	else d.contractmoney 
 end) sum_contractmoney 
from 
(select distinct upParamCode upParamCode,contractno contractno,contractmoney contractmoney from hbase071) d 
group by d.upParamCode;

---其他
create table hbase_other072 as 
select concat(  
case 
when upParamCode = ''  
then "0" 
else upParamCode 
end) as key, COUNT(busicode) busi_num,SUM(money) money,SUM(balAmt) balAmt,
SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt, 
SUM(inDebtAmt) inDebtAmt, SUM(outDebtAmt) outDebtAmt
from hbase071  group by upParamCode;

---总统计
create table hbase072 row format delimited fields terminated by '\t' stored as textfile as 
select hcn.key key,hcn.cust_num cust_num,ho.busi_num busi_num,hcm.sum_contractmoney contractmoney,ho.money money,ho.balAmt balAmt, 
ho.payDebt payDebt, ho.inDebtAmt inDebtAmt, ho.outDebtAmt outDebtAmt 
from hbase_custnum072 hcn join hbase_contractmoney072 hcm on hcn.key =hcm.key join hbase_other072 ho on hcm.key = ho.key;

drop table hbase_custnum072;
drop table hbase_contractmoney072;
drop table hbase_other072;