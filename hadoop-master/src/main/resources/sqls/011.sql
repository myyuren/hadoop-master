--省级全量查询明细 台帐表与机构关联 无distinct
use out;
   SELECT 
--distinct 
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
	from miloancard mlc 
	join xcrms_grouprelation r on mlc.deptcode = r.lowgroupid 
	--join GCASSUREMAIN main on mlc.CONTRACTNO = main.UPKEYCODE 
	--join GCASSUREMULTICLIENT client on client.keycode = main.keycode 
	where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816' AND r.upgroupid = '000000' 
	ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 1000;