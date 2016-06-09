/********************************************************
*
* Name: DM_INVOICE_ITM_INFO_PROC
* Created by: RH, 5/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_INVOICE_ITM_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

declare
-- CREATE OR REPLACE PROCEDURE DM_INVOICE_ITM_INFO_PROC IS

TYPE DM_INVOICE_ITM_INFO_TYP IS TABLE OF DM_INVOICE_ITM_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_INVOICE_ITM_INFO_tab DM_INVOICE_ITM_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    di.ACCT_NUM ACCOUNT_NUMBER -- ST_DOCUMENT_MAILING_EXCEPTION
    ,di.DOCUMENT_ID INVOICE_NUMBER
    ,'POSTPAID' CATEGORY  -- DEFAULT to 'Postpaid'

    ,CASE WHEN va.BANKRUPTCY_FLAG is NOT null 
          THEN 'BKTY'  -- Bankruptcy flag in VB_Activity to BKTY to get this status, 
    
--OTHER (balance adjustments) - 
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('8') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '8' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'OTHER'

--INVTOLL
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('38','42') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID in ('38','42') and di.DOC_TYPE_ID in ('1','2') 
          THEN 'INVTOLL'

--ESCTOLL
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('38','42') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN vac.COLL_COURT_FLAG is null and vac.KS_LEDGER_EVENT_TYPE_ID in ('38','42') and di.DOC_TYPE_ID in ('1','2') 
          THEN 'ESCTOLL'

--CLTOLL (collection toll)
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('38','42') and COLL_COURT_FLAG = 'COLL' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Collection assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG = 'COLL' and vac.KS_LEDGER_EVENT_TYPE_ID in ('38','42') and di.DOC_TYPE_ID in ('1','2') 
          THEN 'CLTOLL' --(collection toll)

--UTCTOLL
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('38','42') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID ='3'
          WHEN vac.COLL_COURT_FLAG is null and vac.KS_LEDGER_EVENT_TYPE_ID in ('38','42') and di.DOC_TYPE_ID = '3' 
          THEN 'UTCTOLL'

--COURTTOLL
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('38','42') and COLL_COURT_FLAG = 'CRT' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Court assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG = 'CRT' and vac.KS_LEDGER_EVENT_TYPE_ID in ('38','42') and di.DOC_TYPE_ID in ('1','2') 
          THEN 'COURTTOLL'

--COURTPLEATOLL
--Join VB_ACTIVITY.KS_LEDGER_EVENT_TYPE_ID ='89' and COLL_COURT_FLAG is null and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') 
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '89'  and di.DOC_TYPE_ID in ('1','2') 
          THEN 'COURTPLEATOLL'

----REGHOLDTOLL
----Join ST_ACCT_FLAGS.ACCT_NUM = ST_document_info.acct_num and ST_ACCT_FLAG.FLAG_TYPE =9 and END_DATE is null, 
----All the following tolls are on REG HOLD
----JOIN ID of KS_LEDGER to LEDGER_ID of VB_ACTVITY for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and 
----JOIN PA_LANE_TXN_ID of KS_LEDGER to TXN_ID of PA_LANE_LANE_TXN and JOIN PLAZA_ID of PA_PLAZA to EXT_PLAZA_ID for PA_Lane_txn
----  IF SUM (VB_ACTIVITY.AMT_CHARGED  VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS grouped by KS_LEDGER.ACCT_NUM
----  FOR PA_PLAZA.FTE_PLAZA = 'Y',
----      VB_ACTIVITY.COLL_COURT_FLAG is NULL, 
----      VB_ACTIVITY.CHILD_DOC_ID not null,
----      VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and 
----      VB_ACTIVITY.BANKRUPTCY_flag is null
----  THEN 'REGSTOP'
--          WHEN va.COLL_COURT_FLAG is null and va.CHILD_DOC_ID not null 
----      VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and 
--          
--          va.KS_LEDGER_EVENT_TYPE_ID = '89'  and di.DOC_TYPE_ID in ('1','2') 
--          THEN 'REGSTOP'
          
  
--ADMINFEE
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('48') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '48' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'ADMINFEE'

--ESCADMINFEE
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('48') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN vac.COLL_COURT_FLAG is null and vac.KS_LEDGER_EVENT_TYPE_ID = '48' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'ESCADMINFEE'

--CLADMINFEE
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('48') and COLL_COURT_FLAG = 'COLL' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Collection assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG  = 'COLL' and vac.KS_LEDGER_EVENT_TYPE_ID = '48' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'CLADMINFEE'

--UTCFEE
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('25') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID ='3'
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '25' and di.DOC_TYPE_ID = '3' 
          THEN 'UTCFEE'

--CRTPLEAFEE
--Join VB_ACTIVITY.KS_LEDGER_EVENT_TYPE_ID ='100' and COLL_COURT_FLAG is null and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '100' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'CRTPLEAFEE'

--COURTUTCFEE
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('25') and COLL_COURT_FLAG = 'CRT' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Court assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG = 'CRT' and vac.KS_LEDGER_EVENT_TYPE_ID = '25' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'COURTUTCFEE'

--EXPLANECHARGE (express lane charge)
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('145') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '145' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'EXPLANECHARGE' --(express lane charge)

--ESCEXPPLANCECHARGE
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('145') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN vac.COLL_COURT_FLAG is null and vac.KS_LEDGER_EVENT_TYPE_ID = '145' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'ESCEXPPLANCECHARGE'

--MAILFEE (include a statement fee in case this is in the data)- We do not do this currently

--NSF
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('17') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN va.COLL_COURT_FLAG is null and va.KS_LEDGER_EVENT_TYPE_ID = '17' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'NSF'

--ESCNSF
--FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('17') and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2')
          WHEN vac.COLL_COURT_FLAG is null and vac.KS_LEDGER_EVENT_TYPE_ID = '17' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'ESCNSF'

--CLNSF
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('17') and COLL_COURT_FLAG = 'COLL' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Collection assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG = 'COLL' and vac.KS_LEDGER_EVENT_TYPE_ID = '17' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'CLNSF'

--CLEXPLANECHARGE
--VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in ('145') and COLL_COURT_FLAG = 'COLL' and Bankruptcy_flag is null. 
--JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY 
--where ST_DOCUMENT_INFO.DOC_TYPE_ID in ('1','2') gives latest document  (Collection assignment is not associated to a document creation )
          WHEN vac.COLL_COURT_FLAG = 'COLL' and vac.KS_LEDGER_EVENT_TYPE_ID = '145' and di.DOC_TYPE_ID in ('1','2') 
          THEN 'CLEXPLANECHARGE'
          
--REGHOLDTOLL   ----------------------------------------------------------------  Clairification ?
-- Join ST_ACCT_FLAGS.ACCT_NUM = ST_document_info.acct_num  
--  and ST_ACCT_FLAGS.FLAG_TYPE ='9' and END_DATE is null, 
--   All the following tolls are on REG HOLD
--JOIN ID             of KS_LEDGER to LEDGER_ID of VB_ACTVITY       for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and 
--JOIN PA_LANE_TXN_ID of KS_LEDGER to TXN_ID    of PA_LANE_LANE_TXN and
--JOIN PLAZA_ID       of PA_PLAZA  to EXT_PLAZA_ID for PA_Lane_txn
--   when --IF SUM (VB_ACTIVITY.AMT_CHARGED  VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS grouped by KS_LEDGER.ACCT_NUM
--  FOR PA_PLAZA.FTE_PLAZA = 'Y',
--  VB_ACTIVITY.COLL_COURT_FLAG is NULL, 
--  VB_ACTIVITY.CHILD_DOC_ID not null,
--  VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and 
--  VB_ACTIVITY.BANKRUPTCY_flag is null
--          THEN 'REGSTOP'

--**Discussed bankruptcy, deceased, court  these will be tracked as statuses 
--For ST_document_info. Acct_num joined PA_ACCT_DETAIL.DECEASED_DATE -- CRT is provided above
          ELSE 'None'
      END SUB_CATEGORY  -- Derived
      
    ,'Mapping' STATUS
    ,trunc(NVL(di.CREATED_ON,SYSDATE)) INVOICE_DATE
    ,(di.PREV_DUE + di.TOLL_CHARGED + di.FEE_CHARGED + di.PAYMT_ADJS) PAYABLE
    ,0 INVOICE_ITEM_NUMBER
    ,nvl((select unique kl.PA_LANE_TXN_ID from KS_LEDGER kl
        where kl.ID = va.LEDGER_ID),0) LANE_TX_ID
    ,'Open' LEVEL_INFO
    ,di.PROMOTION_STATUS REASON_CODE  --PROMOTION_CODE
    ,'INVOICE-ITEM' INVOICE_TYPE
    ,0 PAID_AMOUNT
    ,di.CREATED_ON  CREATED
    ,NULL CREATED_BY
    ,SYSDATE LAST_UPD
    ,NULL LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
FROM PATRON.ST_DOCUMENT_INFO di
      ,PATRON.VB_ACTIVITY va
      ,PATRON.VB_ACTIVITY vac
  WHERE di.DOCUMENT_ID = va.DOCUMENT_ID -- (+)
    AND di.DOCUMENT_ID = vac.CHILD_DOC_ID (+) 
    and rownum<1101
    ; -- Source

SQL_STRING  varchar2(500) := 'truncate table ';
LOAD_TAB    varchar2(50)  := 'DM_INVOICE_ITM_INFO';
ROW_CNT NUMBER := 0;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_INVOICE_ITM_INFO_tab
    LIMIT P_ARRAY_SIZE;

    FOR i IN 1 .. DM_INVOICE_ITM_INFO_tab.COUNT LOOP

-- REGHOLDTOLL   -------------------------------------------------------------  Clairification ?
-- Join ST_ACCT_FLAGS.ACCT_NUM = ST_document_info.acct_num  
--  and ST_ACCT_FLAGS.FLAG_TYPE ='9' and END_DATE is null, 
--   All the following tolls are on REG HOLD
--JOIN ID             of KS_LEDGER to LEDGER_ID of VB_ACTVITY       for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and 
--JOIN PA_LANE_TXN_ID of KS_LEDGER to TXN_ID    of PA_LANE_LANE_TXN and
--JOIN PLAZA_ID       of PA_PLAZA  to EXT_PLAZA_ID for PA_Lane_txn
--          
--when --IF SUM (VB_ACTIVITY.AMT_CHARGED  VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS 
--  grouped by KS_LEDGER.ACCT_NUM
--  FOR PA_PLAZA.FTE_PLAZA = 'Y',
--  VB_ACTIVITY.COLL_COURT_FLAG is NULL, 
--  VB_ACTIVITY.CHILD_DOC_ID not null,
--  VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and 
--  VB_ACTIVITY.BANKRUPTCY_flag is null
--          THEN 'REGSTOP'

      begin
--        select max(sc.STATUS_CHG_DATE) into  DM_ACCOUNT_INFO_tab(i).SUB_CATEGORY
--        from PATRON.PA_ACCT_STATUS_CHANGES sc
--        where sc.ACCT_NUM = DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
--        ;
        if DM_ACCOUNT_INFO_tab(i).SUB_CATEGORY is null then
          DM_ACCOUNT_INFO_tab(i).SUB_CATEGORY := 'REGSTOP2';
        End if;

      exception 
        when others then null;
--        DM_INVOICE_ITM_INFO_tab(i).SUB_CATEGORY:=null;
      end;

--    DBMS_OUTPUT.PUT_LINE('ACCT- '||i||' - '||DM_INVOICE_ITM_INFO_tab(i).ACCOUNT_NUMBER
--        ||' - '||DM_INVOICE_ITM_INFO_tab(i).ACCOUNT_STATUS_DATETIME
--        ||' - '||DM_INVOICE_ITM_INFO_tab(i).LAST_INVOICE_DATE);
           
--    IF ACCOUNT_NUM EXISTS IN ST_REG_STOP_PAYMENT_PLAN AND NVL(PAYMENT_PLAN_END, TRUNC(SYSDATE))  >=  TRUNC(SYSDATE) 
--    THEN 'A'    -- (if plan end date is future or null) 
--    ELSE 'N' 
--    ,case when pa.ACCT_NUM = (select ACCT_NUM 
--                            from PATRON.ST_REG_STOP_PAYMENT_PLAN rsp
--                            where rsp.ACCT_NUM = pa.ACCT_NUM
--                            and nvl(PAYMENT_PLAN_END, trunc(SYSDATE)) >= trunc(SYSDATE)) THEN 'A' 
--                        
--          else 'N'

    END LOOP;
 
    /*  ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_INVOICE_ITM_INFO_tab.first .. DM_INVOICE_ITM_INFO_tab.last
           INSERT INTO DM_INVOICE_ITM_INFO VALUES DM_INVOICE_ITM_INFO_tab(i);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;

  COMMIT;

  CLOSE C1;

  COMMIT;

  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
END;
/
SHOW ERRORS


/*

OTHER (balance adjustments) - FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (8) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2)

INVTOLL
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (38,42) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2)

ESCTOLL
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (38,42) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2)

CLTOLL (collection toll)
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (38,42) and COLL_COURT_FLAG = COLL and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2) 
gives latest document  (Collection assignment is not associated to a document creation )

UTCTOLL
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (38,42) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID =3

COURTTOLL
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (38,42) and COLL_COURT_FLAG = CRT and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2) 
gives latest document  (Court assignment is not associated to a document creation )

COURTPLEATOLL
Join VB_ACTIVITY.KS_LEDGER_EVENT_TYPE_ID =89 and COLL_COURT_FLAG is null and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2) 

REGHOLDTOLL
Join ST_ACCT_FLAGS.ACCT_NUM = ST_document_info.acct_num and ST_ACCT_FLAG.FLAG_TYPE =9 and END_DATE is null, 
All the following tolls are on REG HOLD
JOIN ID of KS_LEDGER to LEDGER_ID of VB_ACTVITY for KS_LEDGER.TRANSACTION_TYPE in ('38','42') and 
JOIN PA_LANE_TXN_ID of KS_LEDGER to TXN_ID of PA_LANE_LANE_TXN and JOIN PLAZA_ID of PA_PLAZA to EXT_PLAZA_ID for PA_Lane_txn
  IF SUM (VB_ACTIVITY.AMT_CHARGED  VB_ACTIVITY.TOTAL_AMT_PAID) < :REG_STOP_THRESHOLD_AMT_IN_CENTS grouped by KS_LEDGER.ACCT_NUM
  FOR PA_PLAZA.FTE_PLAZA = 'Y',VB_ACTIVITY.COLL_COURT_FLAG is NULL, VB_ACTIVITY.CHILD_DOC_ID not null,VB_ACTIVITY.CHILD_DOC_ID NOT LIKE '%-%' and VB_ACTIVITY.BANKRUPTCY_flag is null
  THEN 'REG STOP'
  
ADMINFEE
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (48) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2)

ESCADMINFEE
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (48) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2)

CLADMINFEE
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (48) and COLL_COURT_FLAG = COLL and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2) 
gives latest document  (Collection assignment is not associated to a document creation )

UTCFEE
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (25) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID =3

CRTPLEAFEE
Join VB_ACTIVITY.KS_LEDGER_EVENT_TYPE_ID =100 and COLL_COURT_FLAG is null and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2)

COURTUTCFEE
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (25) and COLL_COURT_FLAG = CRT and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2) 
gives latest document  (Court assignment is not associated to a document creation )

EXPLANECHARGE (express lane charge)
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (145) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2)

ESCEXPPLANCECHARGE
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (145) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2)

MAILFEE (include a statement fee in case this is in the data)- We do not do this currently

NSF
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (17) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w DOCUMENT_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2)

ESCNSF
FOR VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (17) and COLL_COURT_FLAG is null and Bankruptcy_flag is null 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2)

CLNSF
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (17) and COLL_COURT_FLAG = COLL and Bankruptcy_flag is null. 
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2) 
gives latest document  (Collection assignment is not associated to a document creation )

CLEXPLANECHARGE
VB_ACTIVITY KS_LEDGER_EVENT_TYPE_ID in (145) and COLL_COURT_FLAG = COLL and Bankruptcy_flag is null.
JOIN DOCUMENT_ID of ST_DOCUMENT_INFO w CHILD_DOC_ID of VB_ACTIVITY where ST_DOCUMENT_INFO.DOC_TYPE_ID in (1,2) 
gives latest document  (Collection assignment is not associated to a document creation )


**Discussed bankruptcy, deceased, court  these will be tracked as statuses  
use Bankruptcy flag in VB_Activity to BKTY in above queries to get this status, 
For ST_document_info. Acct_num joined PA_ACCT_DETAIL.DECEASED_DATE
CRT is provided above
*/


