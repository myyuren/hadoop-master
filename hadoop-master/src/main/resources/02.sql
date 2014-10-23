---网点按贷款形态查询
use process;
SELECT 
	distinct mlc.deptcode,
    busitype,
    mlc.custid,
    mlc.cname,
    mlc.currency,
    mlc.contractmoney,
    mlc.loantype,
    mlc.loanaccount,
    mlc.busicode,
    mlc.loandate,
    mlc.money,
    mlc.enddate,
    mlc.interest,
    mlc.loanpurpose,
    mlc.balamt,
    mlc.indebtamt,
    mlc.outdebtamt,
    mlc.accountstate,
    mlc.fiveclass,
    mlc.tenclass,
    mlc.paymode,
    mlc.mainassure,
    mlc.contractno,
    mlc.busimanager,
    mlc.assistbusimanager   
	from miloancard02 mlc 
	where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816' 
	AND mlc.deptCode = '020740' AND mlc.citycode = '907' 
	AND mlc.fiveClass in ('5','4','3','2','1') 
	ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 1000; 