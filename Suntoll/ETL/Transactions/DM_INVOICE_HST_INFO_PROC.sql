/********************************************************
*
* Name: DM_INVOICE_HST_INFO_PROC
* Created by: RH, 5/31/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_INVOICE_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_INVOICE_HST_INFO_PROC  
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS
P_BEGIN_DATE  DATE;
P_END_DATE    DATE;

TYPE DM_INVOICE_HST_INFO_TYP IS TABLE OF DM_INVOICE_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_INVOICE_HST_INFO_tab DM_INVOICE_HST_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT
    sdi.ACCT_NUM ACCOUNT_NUMBER
    ,sdi.DOCUMENT_ID INVOICE_NUMBER
    ,trunc(sdi.CREATED_ON) INVOICE_DATE  -- date only

--JOIN LEDGER_ID OF ST_ACTIVITY_PAID to ID OF KS_LEDGER 
--  returns ORIGINAL_AMT from KS_LEDGER AND DISPUTED_AMT FROM ST_ACTIVITY_PAID; 
--Use dfference to determine if dismissed or not (IF 0 THEN 'DISPUTED' ELSE 'CLOSED') 
-- DISPUTED_AMT ??

--------------------------------------------------------------------------------
--JOIN DOCUMENT_INFO_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of VB_ACTIVITY 
--  returns UNPAID_TXN_DETAILS  (AMT_CHARGED, TOTAL_AMT_PAID)
--UNION ALL 
--JOIN DOCUMENT_ID      from ST_DOCUMENT_INFO to DOCUMENT_ID of ST_ACTIVITY_PAID 
--  returns PAID_TXN_DETAILS  
--IF UNPAID_AMT > 0 THEN 'OPEN' ELSE 'CLOSED'  (Need PAID_TXN_DETAILS definition from FTE); 
--
--JOIN LEDGER_ID OF ST_ACTIVITY_PAID to ID OF KS_LEDGER 
--  returns ORIGINAL_AMT from KS_LEDGER AND DISPUTED_AMT FROM ST_ACTIVITY_PAID; 
--  Use idfference to determine if dismissed or not (IF 0 THEN 'DISPUTED' ELSE 'CLOSED') 

    ,CASE WHEN (nvl(va.AMT_CHARGED,0) - nvl(va.TOTAL_AMT_PAID,0)) > 0 THEN 'OPEN'
          WHEN (nvl(sap.AMT_CHARGED,0) - nvl(sap.TOTAL_AMT_PAID,0)) > 0 THEN 'OPEN'
          WHEN (select nvl(kl.AMOUNT,0) from KS_LEDGER kl where kl.id = sap.LEDGER_ID)
                - (nvl(sap.AMT_CHARGED,0) - nvl(sap.TOTAL_AMT_PAID,0)) >0  THEN 'DISPUTED'
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
    ,sdi.DOCUMENT_START START_DATE
    ,sdi.DOCUMENT_END END_DATE
    ,sdi.PREV_DUE OPENING_BALANCE
    ,(nvl(sdi.PREV_DUE,0) + nvl(sdi.TOLL_CHARGED,0)
        + nvl(sdi.FEE_CHARGED,0) + nvl(sdi.PAYMT_ADJS,0)) CLOSING_BALANCE

--JOIN DOCUMENT_INFO_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of VB_ACTIVITY 
--  returns PAID_TXN_DETAILS
--UNION ALL 
--JOIN DOCUMENT_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of ST_ACTIVITY_PAID 
--  returns PAID_TXN_DETAILS PAID_AMT + DISMISSED_AMT  ??
-- What is  DISMISSED_AMT ??
    ,(va.TOTAL_AMT_PAID + sap.TOTAL_AMT_PAID) 
        + (sap.AMT_CHARGED - sap.TOTAL_AMT_PAID) 
        + (va.AMT_CHARGED - va.TOTAL_AMT_PAID) CREDITS  -- Derived
        
--SUM(TOLL_CHARGED + FEE_CHARGED)
    ,(nvl(sdi.TOLL_CHARGED,0) + nvl(sdi.FEE_CHARGED,0)) PAYABALE
    
--  SUM OF (PREV_DUE
--    TOLL_CHARGED
--    FEE_CHARGED
--    PAYMT_ADJS)
    ,(nvl(sdi.PREV_DUE,0) + nvl(sdi.TOLL_CHARGED,0) 
        + nvl(sdi.FEE_CHARGED,0) + nvl(sdi.PAYMT_ADJS,0)) INVOICE_AMT
        
--JOIN DOCUMENT_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of VB_ACTIVITY 
--  returns UNPAID_TXN_DETAILS
--UNION ALL JOIN DOCUMENT_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of ST_ACTIVITY_PAID 
--  returns PAID_TXN_DETAILS TOTAL_AMT_PAID 
    ,(nvl(va.TOTAL_AMT_PAID,0) + nvl(sap.TOTAL_AMT_PAID,0)) PAYMENTS 
    ,sdi.PAYMT_ADJS ADJUSTMENTS
    ,'N' IS_ESCALATION_EXMPT
    ,trunc(MAILED_ON)+25 PAYMENT_DUE_DT
    ,NULL DISCOUNT_ELIGIBLE_CHARGES
    ,0 DISCOUNTS
    ,0 MILEGE
    ,NVL(sdi.DOC_TYPE_ID,'INVOICE') INVOICE_TYPE
    ,sdi.CREATED_ON CREATED
    ,'9986' CREATED_BY -- SYSTEM_EMP_CODE
    ,sdi.CREATED_ON LAST_UPD
    ,NULL LAST_UPD_BY -- SYSTEM_EMP_CODE
    ,'SUNTOLL' SOURCE_SYSTEM
--    ,sdi.COVER_IMAGE_VIO_EVENT_ID COVER_IMAGE_VIO_EVENT_ID 
--    ,sdi.ADDRESS_ID ADDRESS_ID  -- PA_ACCT_ADDR.ADDRESS_ID
--    ,sdi.REG_STOP_FLAG REGISTRATION_STOP_FLAG   -- ST_ACCOUNT_FLAGS.REGISTRATION_STOP_FLAG
--    ,sdi.RED_ENVELOPE_FLAG RED_ENVELOPE_FLAG   -- ST_ACCOUNT_FLAGS.RED_ENVELOPE_FLAG
--    ,sdi.VEH_LIC_NUM LICENSE_PLATE_NUM   -- PA_LANE_TXN.VEH_LIC_NUM
--    ,sdi.MAILED_ON MAILED_ON 

--JOIN DOCUMENT_INFO_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of VB_ACTIVITY 
--  returns PAID_TXN_DETAILS
--UNION ALL 
--JOIN DOCUMENT_ID from ST_DOCUMENT_INFO to DOCUMENT_ID of ST_ACTIVITY_PAID 
--  returns PAID_TXN_DETAILS DISMISSED_AMT
--    ,NULL DISMISSED   -- DISMISSED_AMT
  FROM PATRON.ST_DOCUMENT_INFO sdi
      ,PATRON.VB_ACTIVITY va
      ,PATRON.ST_ACTIVITY_PAID sap
  WHERE sdi.DOCUMENT_ID = va.DOCUMENT_ID (+)  
    AND sdi.DOCUMENT_ID = sap.DOCUMENT_ID (+)
--AND   di.ACCT_NUM >= p_begin_acct_num AND   di.ACCT_NUM <= p_end_acct_num
; -- Source

row_cnt          NUMBER := 0;
v_trac_rec       dm_tracking%ROWTYPE;
v_trac_etl_rec   dm_tracking_etl%ROWTYPE;

BEGIN
  SELECT * INTO v_trac_etl_rec
  FROM  dm_tracking_etl
  WHERE track_etl_id = i_trac_id;
  DBMS_OUTPUT.PUT_LINE('Start '||v_trac_etl_rec.etl_name||' ETL load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

  v_trac_etl_rec.status := 'ETL Start ';
  v_trac_etl_rec.proc_start_date := SYSDATE;  
  update_track_proc(v_trac_etl_rec);
 
  SELECT * INTO   v_trac_rec
  FROM   dm_tracking
  WHERE  track_id = v_trac_etl_rec.track_id
  ;

  OPEN C1;   -- (v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_INVOICE_HST_INFO_tab
    LIMIT P_ARRAY_SIZE;

-- ETL SECTION BEGIN

-- ETL SECTION END

    /*Bulk insert */ 
    FORALL i in DM_INVOICE_HST_INFO_tab.first .. DM_INVOICE_HST_INFO_tab.last
           INSERT INTO DM_INVOICE_HST_INFO VALUES DM_INVOICE_HST_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_etl_rec);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('Total ROW_CNT : '||ROW_CNT);

  COMMIT;

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := SQLCODE;
  v_trac_etl_rec.result_msg := SQLERRM;
  v_trac_etl_rec.end_val := v_trac_rec.end_acct;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  
  EXCEPTION
  WHEN OTHERS THEN
    v_trac_etl_rec.result_code := SQLCODE;
    v_trac_etl_rec.result_msg := SQLERRM;
    v_trac_etl_rec.proc_end_date := SYSDATE;
    update_track_proc(v_trac_etl_rec);
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||v_trac_etl_rec.result_code);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
END;
/
SHOW ERRORS
