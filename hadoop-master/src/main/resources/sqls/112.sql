select count(distinct mlc.custid) cust_num 
	from miloancard mlc 
	join xcrms_grouprelation r on mlc.deptcode = r.lowgroupid 
	join pmParamRelation p on p.lowParamCode = mlc.industry 
	join param_class pc on pc.id = p.paramClassId 
	join GCASSUREMAIN main on mlc.CONTRACTNO = main.UPKEYCODE 
	join GCASSUREMULTICLIENT client on client.keycode = main.keycode 
	where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816' AND r.upgroupid = '000000' 
	AND pc.name='party.industry' AND upParamCode='A0000';
		
	select 
	sum(case 
	when d.contractmoney is null 
	then 0 
	else d.contractmoney 
	end) sum_contractmoney from 
	(select distinct mlc.contractno contractno,mlc.contractmoney contractmoney 
	from miloancard mlc 
	join xcrms_grouprelation r on mlc.deptcode = r.lowgroupid 
	join pmParamRelation p on p.lowParamCode = mlc.industry 
	join param_class pc on pc.id = p.paramClassId 
	join GCASSUREMAIN main on mlc.CONTRACTNO = main.UPKEYCODE 
	join GCASSUREMULTICLIENT client on client.keycode = main.keycode 
	where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816' AND r.upgroupid = '000000' 
	AND pc.name='party.industry' AND upParamCode='A0000') d;
	
	select COUNT(d.busicode) busi_num,SUM(d.money) money,SUM(d.balAmt) balAmt,
	SUM(
        CASE 
		WHEN d.paydebt IS NULL 
		THEN 0 
		ELSE d.paydebt 
	END) payDebt, SUM(d.inDebtAmt) inDebtAmt, SUM(d.outDebtAmt) outDebtAmt 
	from (SELECT 
	distinct mlc.custid custid,
            mlc.busicode busicode,
            mlc.contractmoney contractmoney,
            mlc.money money,
            mlc.balamt balamt,
            mlc.paydebt paydebt,
            mlc.indebtamt indebtamt,
            mlc.outdebtamt outdebtamt
	from miloancard mlc 
	join xcrms_grouprelation r on mlc.deptcode = r.lowgroupid 
	join pmParamRelation p on p.lowParamCode = mlc.industry 
	join param_class pc on pc.id = p.paramClassId 
	join GCASSUREMAIN main on mlc.CONTRACTNO = main.UPKEYCODE 
	join GCASSUREMULTICLIENT client on client.keycode = main.keycode 
	where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816' AND r.upgroupid = '000000' 
	AND pc.name='party.industry' AND upParamCode='A0000') d;