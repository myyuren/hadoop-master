use process;
drop table if exists export071;
create table export071(
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
 deptcode	 string, 
 paydebt            double, 
 item               string, 
 limittype          string, 
 custtype           string, 
 floatrate          double, 
 industry           string
)
 row format delimited fields terminated by ','
 stored as parquet;

insert overwrite export071
select distinct c.*
from hbase011 c join pmParamRelation p on p.lowParamCode = c.loanPurpose and p.paramClassId = '00257' and p.upparamcode = 'A00000000';

