select count(distinct mlc.custid) cust_num 
		from miloancard mlc 
		join report.xcrms_grouprelation r on mlc.deptcode = r.lowgroupid 
		where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' 
		AND mlc.BMCFG !='816' AND r.upgroupid = '000000' 
		AND mlc.fiveClass in ('5','4','3','2','1');
		
	select 
	sum(case 
	when d.contractmoney is null 
	then 0 
	else d.contractmoney 
	end) sum_contractmoney from 
	(select distinct mlc.contractno contractno,mlc.contractmoney contractmoney 
	from miloancard mlc 
	join report.xcrms_grouprelation r on mlc.deptcode = r.lowgroupid 
	where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' 
	AND mlc.BMCFG !='816' AND r.upgroupid = '000000' 
	AND mlc.fiveClass in ('5','4','3','2','1')) d;
	
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
	join report.xcrms_grouprelation r on mlc.deptcode = r.lowgroupid 
	where ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816' AND r.upgroupid = '000000' 
	AND mlc.fiveClass in ('5','4','3','2','1')) d;