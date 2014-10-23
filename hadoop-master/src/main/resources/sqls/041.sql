SELECT 
	mlc.deptcode,
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
	from GCASSUREMULTICLIENT client
	join GCASSUREMAIN main  on client.keycode = main.keycode 
	join miloancard02 mlc on mlc.CONTRACTNO = main.UPKEYCODE
	where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816' 
	AND mlc.deptCode = '020740' AND CityCode = '907' 
	ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 1000;