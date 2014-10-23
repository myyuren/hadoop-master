use process;

drop table if exists hbase111;
drop table if exists hbase_custnum112;
drop table if exists hbase_contractmoney112;
drop table if exists hbase_other112;
drop table if exists hbase112;

create table hbase111(
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

insert overwrite hbase111
select distinct c.*,p.upparamcode from hbase011 c join pmParamRelation p on p.lowParamCode = c.industry and p.paramClassId = '00243';


---客户总数
create table hbase_custnum112 as 
select concat( 
case 
when d.upParamCode = ''  
then "0" 
else d.upParamCode 
end) as key,count(*) cust_num 
from 
(select distinct upParamCode upParamCode,custid custid from hbase111) d 
group by d.upParamCode;

---合同总金额
create table hbase_contractmoney112 as 
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
(select distinct upParamCode upParamCode,contractno contractno,contractmoney contractmoney from hbase111) d 
group by d.upParamCode;

---其他
create table hbase_other112 as 
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
from hbase111  group by upParamCode;

---总统计
create table hbase112 row format delimited fields terminated by '\t' stored as textfile as 
select hcn.key key,hcn.cust_num cust_num,ho.busi_num busi_num,hcm.sum_contractmoney contractmoney,ho.money money,ho.balAmt balAmt, 
ho.payDebt payDebt, ho.inDebtAmt inDebtAmt, ho.outDebtAmt outDebtAmt 
from hbase_custnum112 hcn join hbase_contractmoney112 hcm on hcn.key =hcm.key join hbase_other112 ho on hcm.key = ho.key;

drop table hbase_custnum112;
drop table hbase_contractmoney112;
drop table hbase_other112;