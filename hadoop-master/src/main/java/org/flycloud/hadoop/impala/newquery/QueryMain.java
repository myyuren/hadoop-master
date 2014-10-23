package org.flycloud.hadoop.impala.newquery;

import org.flycloud.hadoop.impala.thread.Director;

public class QueryMain {
	private static final String SQL_01 = "  select distinct mlc.deptcode, mlc.busitype, mlc.custid, mlc.cname, mlc.currency, mlc.contractmoney, mlc.loantype, "
			+ "mlc.loanaccount, mlc.busicode, mlc.loandate, mlc.money, mlc.enddate, mlc.interest,                                 "
			+ "mlc.loanpurpose, mlc.balamt, mlc.indebtamt, mlc.outdebtamt, mlc.accountstate, mlc.fiveclass,                       "
			+ "mlc.tenclass, mlc.paymode, mlc.mainassure, mlc.contractno, mlc.busimanager,                                        "
			+ "mlc.assistbusimanager                                                                                              "
			+ "FROM out.miloancard mlc join out.xcrms_grouprelation r on mlc.deptcode = r.lowgroupid and r.upgroupid = '000000' "
			+ "AND ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816'                          "
			+ "AND mlc.fiveClass in ('5','4','3','2','1')                                                "
			+ "ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 5                                                        ";

	private static final String SQL_02 = "  select distinct mlc.deptcode, mlc.busitype, mlc.custid, mlc.cname, mlc.currency, mlc.contractmoney, mlc.loantype, "
			+ "mlc.loanaccount, mlc.busicode, mlc.loandate, mlc.money, mlc.enddate, mlc.interest,                                 "
			+ "mlc.loanpurpose, mlc.balamt, mlc.indebtamt, mlc.outdebtamt, mlc.accountstate, mlc.fiveclass,                       "
			+ "mlc.tenclass, mlc.paymode, mlc.mainassure, mlc.contractno, mlc.busimanager,                                        "
			+ "mlc.assistbusimanager                                                                                              "
			+ "FROM report.miloancard02 mlc join report.xcrms_grouprelation r on mlc.deptcode = r.lowgroupid and                  "
			+ "r.upgroupid = '02000A' AND ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816'                          "
			+ "AND mlc.cityCode = '907' AND mlc.fiveClass in ('5','4','3','2','1')                                                "
			+ "ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 5                                                        ";

	private static final String SQL_03 = "  select distinct mlc.deptcode, mlc.busitype, mlc.custid, mlc.cname, mlc.currency, mlc.contractmoney, mlc.loantype, "
			+ "mlc.loanaccount, mlc.busicode, mlc.loandate, mlc.money, mlc.enddate, mlc.interest,                                 "
			+ "mlc.loanpurpose, mlc.balamt, mlc.indebtamt, mlc.outdebtamt, mlc.accountstate, mlc.fiveclass,                       "
			+ "mlc.tenclass, mlc.paymode, mlc.mainassure, mlc.contractno, mlc.busimanager,                                        "
			+ "mlc.assistbusimanager                                                                                              "
			+ "FROM report.miloancard02 mlc join report.xcrms_grouprelation r on mlc.deptcode = r.lowgroupid                  "
			+ "and r.upgroupid = '020594' AND ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816'                          "
			+ "AND mlc.cityCode = '907' AND mlc.fiveClass in ('5','4','3','2','1')                                                "
			+ "ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 5                                                        ";

	private static final String SQL_04 = "  select distinct mlc.deptcode, mlc.busitype, mlc.custid, mlc.cname, mlc.currency, mlc.contractmoney, mlc.loantype, "
			+ "mlc.loanaccount, mlc.busicode, mlc.loandate, mlc.money, mlc.enddate, mlc.interest,                                 "
			+ "mlc.loanpurpose, mlc.balamt, mlc.indebtamt, mlc.outdebtamt, mlc.accountstate, mlc.fiveclass,                       "
			+ "mlc.tenclass, mlc.paymode, mlc.mainassure, mlc.contractno, mlc.busimanager,                                        "
			+ "mlc.assistbusimanager                                                                                              "
			+ "FROM report.miloancard02 mlc                                                                                       "
			+ "where mlc.deptcode = '020740' AND ISOUTTABLELOAN ='F' AND mlc.BMCFG!='813' AND mlc.BMCFG !='816'                          "
			+ "AND mlc.cityCode = '907' AND mlc.fiveClass in ('5','4','3','2','1')                                                "
			+ "ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc limit 5                                                        ";

	public static void main(String[] args) throws InterruptedException {
		Director dir = new Director();
		dir.setMaxCount(100);
		QueryThreadFactory fac = new QueryThreadFactory();
		fac.setSql(SQL_04);
		fac.setDirector(dir);
		dir.setThreadFactory(fac);
		dir.begin();
		Thread.sleep(1*60*1000);
		dir.stp();
	}

}
