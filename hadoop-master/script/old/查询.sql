1、省级用户全量查询：
   1.1  明细查询sql

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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.CONTRACTNO = main.UPKEYCODE
    AND client.keycode = main.keycode
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    1.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc,
                GCASSUREMAIN main,
                                  GCASSUREMULTICLIENT client
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
                              AND mlc.CONTRACTNO = main.UPKEYCODE
                              AND client.keycode = main.keycode
    )

2、市级用户全量查询：
   2.1  明细查询sql

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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.instCityCode = '907'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '02000A')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.CONTRACTNO = main.UPKEYCODE
    AND client.keycode = main.keycode
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    2.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc,
                GCASSUREMAIN main,
                                  GCASSUREMULTICLIENT client
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.instCityCode = '907'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '02000A')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
                              AND mlc.CONTRACTNO = main.UPKEYCODE
                              AND client.keycode = main.keycode
    )

3、县级用户全量查询：
   3.1  明细查询sql

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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.instCityCode = '907'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '020594')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.CONTRACTNO = main.UPKEYCODE
    AND client.keycode = main.keycode
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    3.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc,
                GCASSUREMAIN main,
                                  GCASSUREMULTICLIENT client
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.instCityCode = '907'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '020594')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
                              AND mlc.CONTRACTNO = main.UPKEYCODE
                              AND client.keycode = main.keycode
    )

 4、网点级用户全量查询：
   4.1  明细查询sql

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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.instCityCode = '907'
    AND mlc.busimanager = '02074000003'
    AND mlc.deptCode = '020740'
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.CONTRACTNO = main.UPKEYCODE
    AND client.keycode = main.keycode
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    4.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc,
                GCASSUREMAIN main,
                                  GCASSUREMULTICLIENT client
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.instCityCode = '907'
            AND mlc.busimanager = '02074000003'
            AND mlc.deptCode = '020740'
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
                              AND mlc.CONTRACTNO = main.UPKEYCODE
                              AND client.keycode = main.keycode
    )

 5、省级用户涉农贷款

   5.1  明细查询sql

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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.item in ('999LN000015','999LN000016','999LN000026','999LN000027','999LN000087','999LN000088')
    AND mlc.CONTRACTNO = main.UPKEYCODE
    AND client.keycode = main.keycode
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    5.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc,
                GCASSUREMAIN main,
                                  GCASSUREMULTICLIENT client
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
            AND mlc.item in ('999LN000015','999LN000016','999LN000026','999LN000027','999LN000087','999LN000088')
                              AND mlc.CONTRACTNO = main.UPKEYCODE
                              AND client.keycode = main.keycode
    )

   6、省级用户按客户类型

    6.1  明细查询sql

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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.custType = '01'
    AND mlc.CONTRACTNO = main.UPKEYCODE
    AND client.keycode = main.keycode
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    6.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc,
                GCASSUREMAIN main,
                                  GCASSUREMULTICLIENT client
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
            AND mlc.custType = '01'
                              AND mlc.CONTRACTNO = main.UPKEYCODE
                              AND client.keycode = main.keycode
    )


  7、省级用户按贷款用途

   7.1  明细查询sql

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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.loanPurpose in (select lowParamCode from pmParamRelation where  paramClassId in(select id from param_class where name='cmisgc.loanPurpose') and  upParamCode='A00000000')
    AND mlc.CONTRACTNO = main.UPKEYCODE
    AND client.keycode = main.keycode
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    7.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc,
                GCASSUREMAIN main,
                                  GCASSUREMULTICLIENT client
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
            AND mlc.loanPurpose in (select lowParamCode from pmParamRelation where  paramClassId in(select id from param_class where name='cmisgc.loanPurpose') and  upParamCode='A00000000')
                              AND mlc.CONTRACTNO = main.UPKEYCODE
                              AND client.keycode = main.keycode
    )

  8、省级用户按担保方式
   8.1  明细查询sql

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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.MAINASSURE IN ('C101','C102','C103','C100')
    AND mlc.CONTRACTNO = main.UPKEYCODE
    AND client.keycode = main.keycode
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    8.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc,
                GCASSUREMAIN main,
                                  GCASSUREMULTICLIENT client
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
            AND mlc.MAINASSURE IN ('C101','C102','C103','C100')
                              AND mlc.CONTRACTNO = main.UPKEYCODE
                              AND client.keycode = main.keycode
    )
  9、省级用户按贷款形态
   9.1  明细查询sql

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
  FROM
    miloancard mlc
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.fiveClass in ('5','4','3','2','1')
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    9.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
            AND mlc.fiveClass in ('5','4','3','2','1')
    )
  10、省级用户按账户状态
   10.1  明细查询sql

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
  FROM
    miloancard mlc
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.ACCOUNTSTATE IN ('1','6','2','3','5')
  ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    10.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
            AND mlc.ACCOUNTSTATE IN ('1','6','2','3','5')

    )
  11、省级用户按行业分类
   11.1  明细查询sql

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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.industry in (select lowParamCode from pmParamRelation where paramClassId in (select id from param_class where name='party.industry') and  upParamCode='A0000')
    AND mlc.CONTRACTNO = main.UPKEYCODE
    AND client.keycode = main.keycode
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    11.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc,
                GCASSUREMAIN main,
                                  GCASSUREMULTICLIENT client
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
            AND mlc.industry in (select lowParamCode from pmParamRelation where paramClassId in (select id from param_class where name='party.industry') and  upParamCode='A0000')
                              AND mlc.CONTRACTNO = main.UPKEYCODE
                              AND client.keycode = main.keycode
    )
  12、省级用户按贷款期限
   12.1  明细查询sql

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
  FROM
    miloancard mlc
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.LIMITTYPE in ('1','2','3')
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    12.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
            AND mlc.LIMITTYPE in ('1','2','3')
    )
  13、省级用户按利率浮动
   13.1  明细查询sql

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
  FROM
    miloancard mlc
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.FLOATRATE >= 1.02 and mlc.FLOATRATE <= 105.6874
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    13.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.deptCode in (SELECT LOWGROUPID FROM XCRMS_GROUPRELATION WHERE UPGROUPID= '000000')
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
            AND mlc.FLOATRATE >= 1.02 and mlc.FLOATRATE <= 105.6874
    )
  14、县级用户按客户经理
  14.1  明细查询sql

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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.instCityCode = '907'
    AND mlc.busimanager = '02074000003'
    AND mlc.deptCode = '020740'
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    14.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            distinct mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc,
                GCASSUREMAIN main,
                                  GCASSUREMULTICLIENT client
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.instCityCode = '907'
            AND mlc.busimanager = '02074000003'
            AND mlc.deptCode = '020740'
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
    )

  15、网点用户按贷款帐号
  15.1  明细查询sql
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
  FROM
    miloancard mlc,
          GCASSUREMAIN main,
          GCASSUREMULTICLIENT client
  WHERE
    ISOUTTABLELOAN ='F'
    AND mlc.instCityCode = '907'
    AND mlc.busimanager = '02074000003'
    AND mlc.deptCode = '020740'
    AND mlc.LOANACCOUNT = '9070107104348080085277'
    AND mlc.BMCFG!='813'
    AND mlc.BMCFG !='816'
    AND mlc.CONTRACTNO = main.UPKEYCODE
    AND client.keycode = main.keycode
    ORDER BY CONTRACTMONEY ,LOANDATE ,BALAMT,MONEY desc;

    15.2 汇总查询sql
  SELECT
    COUNT(
        CASE
            WHEN custseq=1
            THEN custid
            ELSE NULL
        END) cust_num,
    COUNT(busicode) busi_num,
    SUM(
        CASE
            WHEN conseq=1
            THEN contractmoney
            ELSE 0
        END) contractmoney,
    SUM(money) money,
    SUM(balAmt) balAmt,
    SUM(
        CASE
            WHEN paydebt IS NULL
            THEN 0
            ELSE paydebt
        END) payDebt,
    SUM(inDebtAmt) inDebtAmt,
    SUM(outDebtAmt) outDebtAmt
  FROM
    (
        SELECT
            mlc.custid,
            mlc.busicode,
            mlc.contractmoney,
            mlc.money,
            mlc.balamt,
            mlc.paydebt,
            mlc.indebtamt,
            mlc.outdebtamt,
            row_number() over(PARTITION BY mlc.CONTRACTNO) AS conseq,
            row_number() over(PARTITION BY mlc.custid)     AS custseq
            FROM
                miloancard mlc
            WHERE
                ISOUTTABLELOAN ='F'
            AND mlc.instCityCode = '907'
            AND mlc.busimanager = '02074000003'
            AND mlc.deptCode = '020740'
            AND mlc.LOANACCOUNT = '9070107104348080085277'
            AND mlc.BMCFG!='813'
            AND mlc.BMCFG !='816'
    )
