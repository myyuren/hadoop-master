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
	from miloancard mlc 
	join xcrms_grouprelation r on mlc.deptcode = r.lowgroupid 
	join pmParamRelation p on p.lowParamCode = mlc.industry 
	join param_class pc on pc.id = p.paramClassId 
	join GCASSUREMAIN main on mlc.CONTRACTNO = main.UPKEYCODE 
	join GCASSUREMULTICLIENT client on client.keycode = main.keycode 
	where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816' AND r.upgroupid = '000000' 
	AND pc.name='party.industry' AND upParamCode='A0000'  
	ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 1000;