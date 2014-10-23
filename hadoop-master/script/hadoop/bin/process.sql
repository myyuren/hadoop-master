use process;
drop table if exists mlc_main_client;
create table mlc_main_client stored as parquet as
SELECT distinct
    trim(mlc.busitype) busitype,
    trim(mlc.custid) custid,
    trim(mlc.cname) cname,
    trim(mlc.currency) currency,
    mlc.contractmoney contractmoney,
    trim(mlc.loantype) loantype,
    trim(mlc.loanaccount) loanaccount,
    trim(mlc.busicode) busicode,
    trim(mlc.loandate) loandate,
    mlc.money money,
    trim(mlc.enddate) enddate,
    mlc.interest interest,
    trim(mlc.loanpurpose) loanpurpose,
    mlc.balamt balamt,
    mlc.indebtamt indebtamt,
    mlc.outdebtamt outdebtamt,
    trim(mlc.accountstate) accountstate,
    trim(mlc.fiveclass) fiveclass,
    trim(mlc.tenclass) tenclass,
    trim(mlc.paymode) paymode,
    trim(mlc.mainassure) mainassure,
    trim(mlc.contractno) contractno,
    trim(mlc.busimanager) busimanager,
    trim(mlc.assistbusimanager) assistbusimanager,
    trim(mlc.instcitycode) instcitycode,
    trim(mlc.deptcode) deptcode, 
	mlc.paydebt paydebt, 
	trim(mlc.item) item, 
	trim(mlc.limittype) limittype, 
	trim(mlc.custtype) custtype,
	mlc.floatrate floatrate, 
	trim(mlc.industry) industry 
from miloancard mlc 
--join GCASSUREMAIN main on trim(mlc.CONTRACTNO) = trim(main.UPKEYCODE) join GCASSUREMULTICLIENT client on trim(client.keycode) = trim(main.keycode) 
where trim(ISOUTTABLELOAN) ='F' AND trim(mlc.BMCFG)!='813' AND trim(mlc.BMCFG) !='816'




