/********************************************************
*
* Name: DM_INVOICE_INFO_PROC
* Created by: RH, 5/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_INVOICE_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

DECLARE
--CREATE OR REPLACE PROCEDURE DM_INVOICE_INFO_PROC IS

TYPE DM_INVOICE_INFO_TYP IS TABLE OF DM_INVOICE_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_INVOICE_INFO_tab DM_INVOICE_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    di.ACCT_NUM ACCOUNT_NUMBER
    ,di.DOCUMENT_ID INVOICE_NUMBER
    ,trunc(di.CREATED_ON) INVOICE_DATE  -- date only

--JOIN LEDGER_ID OF ST_ACTIVITY_PAID to ID OF KS_LEDGER 
--  returns ORIGINAL_AMT from KS_LEDGER AND DISPUTED_AMT FROM ST_ACTIVITY_PAID; 
--Use dfference to determine if dismissed or not (IF 0 THEN 'DISPUTED' ELSE 'CLOSED') 
-- DISPUTED_AMT ??
    ,CASE WHEN (nvl(va.AMT_CHARGED,0) - nvl(va.TOTAL_AMT_PAID,0)) > 0 THEN 'OPEN'
          WHEN (nvl(ap.AMT_CHARGED,0) - nvl(ap.TOTAL_AMT_PAID,0)) > 0 THEN 'OPEN'
          WHEN (select nvl(AMOUNT,0) from KS_LEDGER where id = ap.LEDGER_ID)
                - (nvl(ap.AMT_CHARGED,0) - nvl(ap.TOTAL_AMT_PAID,0)) >0  THEN 'DISPUTED'
          ELSE 'CLOSED'
      END STATUS  -- Derived
      
--VB_ACTIVITY table 
--FOR BANKRUPTCY_FLAG NULL and COLL_COURT_FLAG NULL
--IF DOCUMENT_ID is null, then 'UNBILLED'
--IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NULL, then 'INVOICED'
--IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL, then 'ESCALATED'
--IF DOCUMENT_ID is not null and CHILD_DOC_ID like '%-%', then 'UTC'
--FOR BANKRUPTCY_FLAG NULL and COLL_COURT_FLAG NOT NULL
--IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL  and COLL_COURT_FLAG is 'COLL' THEN 'COLLECTION'
--IF DOCUMENT_ID is not null and CHILD_DOC_ID IS NOT NULL  and COLL_COURT_FLAG is 'CRT' THEN 'COURT'
--FOR BANKRUPTCY _FLAG NOT NULL
--IF BANKRUPTCY _FLAG is not null, then 'BANKRUPTCY'
    ,CASE WHEN va.BANKRUPTCY_FLAG is NOT null THEN 'BANKRUPTCY'
          WHEN va.COLL_COURT_FLAG is null and
               va.DOCUMENT_ID is null THEN 'UNBILLED'
          WHEN va.COLL_COURT_FLAG is null and
               va.DOCUMENT_ID is NOT null and
               va.CHILD_DOC_ID is null THEN 'INVOICED'
          WHEN va.COLL_COURT_FLAG is null and
               va.DOCUMENT_ID is NOT null and
               va.CHILD_DOC_ID is NOT null THEN 'ESCALATED'
          WHEN va.COLL_COURT_FLAG is null and
               va.DOCUMENT_ID is NOT null and
               va.CHILD_DOC_ID like '%-%' THEN 'UTC'
          WHEN va.COLL_COURT_FLAG is NOT null and
               va.DOCUMENT_ID is NOT null and
               va.CHILD_DOC_ID is NOT null and
               va.COLL_COURT_FLAG = 'COLL' THEN 'COLLECTION'
          WHEN va.COLL_COURT_FLAG is NOT null and
               va.DOCUMENT_ID is NOT null and
               va.CHILD_DOC_ID is NOT null and
               va.COLL_COURT_FLAG = 'CRT' THEN 'COURT'
          ELSE NULL
      END ESCALATION_LEVEL  -- Derived
    ,di.DOCUMENT_START START_DATE
    ,di.DOCUMENT_END END_DATE
    ,di.PREV_DUE OPENING_BALANCE
    ,(nvl(di.PREV_DUE,0)
        + nvl(di.TOLL_CHARGED,0)
        + nvl(di.FEE_CHARGED,0) 
        + nvl(di.PAYMT_ADJS,0)) CLOSING_BALANCE
    
-- PAID_AMT + DISMISSED_AMT  -- What is  DISMISSED_AMT ??
    ,(nvl(va.TOTAL_AMT_PAID,0) + nvl(ap.TOTAL_AMT_PAID,0))
        + (nvl(ap.AMT_CHARGED,0) - nvl(ap.TOTAL_AMT_PAID,0)) 
        + (nvl(va.AMT_CHARGED,0) - nvl(va.TOTAL_AMT_PAID,0)) CREDITS  -- Derived
    ,(nvl(di.TOLL_CHARGED,0) + nvl(di.FEE_CHARGED,0)) PAYABALE
    ,(nvl(di.PREV_DUE,0) + nvl(di.TOLL_CHARGED,0) + nvl(di.FEE_CHARGED,0) + nvl(di.PAYMT_ADJS,0)) INVOICE_AMT
    ,(nvl(va.TOTAL_AMT_PAID,0) + nvl(ap.TOTAL_AMT_PAID,0)) PAYMENTS 
    ,di.PAYMT_ADJS ADJUSTMENTS
    ,'N' IS_ESCALATION_EXMPT
    ,trunc(MAILED_ON)+25 PAYMENT_DUE_DT
    ,NULL DISCOUNT_ELIGIBLE_CHARGES
    ,0 DISCOUNTS
    ,0 MILEGE
    ,NVL(di.DOC_TYPE_ID,'INVOICE') INVOICE_TYPE
    ,di.CREATED_ON CREATED
    ,'9986' CREATED_BY
    ,di.CREATED_ON LAST_UPD
    ,NULL LAST_UPD_BY
    ,'SUNTOLL' SOURCE_SYSTEM
--    ,di.COVER_IMAGE_VIO_EVENT_ID COVER_IMAGE_VIO_EVENT_ID 
--    ,di.ADDRESS_ID ADDRESS_ID  -- PA_ACCT_ADDR.ADDRESS_ID
--    ,di.REG_STOP_FLAG REGISTRATION_STOP_FLAG   -- ST_ACCOUNT_FLAGS.REGISTRATION_STOP_FLAG
--    ,di.RED_ENVELOPE_FLAG RED_ENVELOPE_FLAG   -- ST_ACCOUNT_FLAGS.RED_ENVELOPE_FLAG
--    ,di.VEH_LIC_NUM LICENSE_PLATE_NUM   -- PA_LANE_TXN.VEH_LIC_NUM
--    ,di.MAILED_ON MAILED_ON 
--    ,NULL DISMISSED   -- DISMISSED_AMT
  FROM PATRON.ST_DOCUMENT_INFO di
      ,PATRON.VB_ACTIVITY va
      ,PATRON.ST_ACTIVITY_PAID ap
  WHERE di.DOCUMENT_ID = va.DOCUMENT_ID (+)  
    AND di.DOCUMENT_ID = ap.DOCUMENT_ID (+)   ; -- Source

SQL_STRING  varchar2(500) := 'truncate table ';
LOAD_TAB    varchar2(50)  := 'DM_INVOICE_INFO';
ROW_CNT NUMBER := 0;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_INVOICE_INFO_tab
    LIMIT P_ARRAY_SIZE;

-- ETL SECTION BEGIN

-- ETL SECTION END

    /*Bulk insert */ 
    FORALL i in DM_INVOICE_INFO_tab.first .. DM_INVOICE_INFO_tab.last
           INSERT INTO DM_INVOICE_INFO VALUES DM_INVOICE_INFO_tab(i);
                       
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


