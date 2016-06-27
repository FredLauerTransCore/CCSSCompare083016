/********************************************************
*
* Name: DM_ACCOUNT_HST_INFO_PROC
* Created by: RH, 5/27/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCOUNT_HST_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

-- RH 6/20/2016 Added Tracking and acct num parameters

CREATE OR REPLACE PROCEDURE DM_ACCOUNT_HST_INFO_PROC 
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_ACCOUNT_HST_INFO_TYP IS TABLE OF DM_ACCOUNT_HST_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCOUNT_HST_INFO_tab DM_ACCOUNT_HST_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT
    pa.ACCT_NUM ACCOUNT_NUMBER
    ,pa.ACCTSTAT_ACCT_STATUS_CODE ACCOUNT_STATUS
    ,NULL ACCOUNT_STATUS_DATETIME -- MAX(STATUS_CHG_DATE) PA_ACCT_STATUS_CHANGES - ETL sec
    ,'MAIL' STATEMENT_DELIVERY_MODE
    ,'NONE' STATEMENT_PERIOD  -- What is the source for this?PA_ACCT.Statement_option MONTHLY
    ,pa.ACCTTYPE_ACCT_TYPE_CODE ACCOUNT_TYPE
    ,pa.CREATED_ON ACCOUNT_OPEN_DATE    -- ACCT_OPEN_DATE 
    ,trim(pa.L_NAME)||', '||trim(pa.F_NAME) ACCOUNT_NAME
    ,pa.ORG COMPANY_NAME
    ,NULL DBA -- N/A
    ,pa.E_MAIL_ADDR EMAIL_ADDRESS
--    ,PA_ACCT_REPL_DETAIL.REPLENISHMENT_AMT REBILL_AMOUNT
    ,0 REBILL_AMOUNT
--    ,PA_ACCT_REPL_DETAIL.LOW_BAL_AMT REBILL_THRESHOLD
    ,0 REBILL_THRESHOLD
    ,pa.ACCT_PIN_NUMBER PIN
    ,NULL VIDEO_ACCT_STATUS -- N/A
    ,NULL SUSPENDED_DATE -- N/A
--    ,pa.CREATED_ON LAST_INVOICE_DATE --  ST_DOCUMENT_INFO - MAX(CREATED_ON)
    ,NULL LAST_INVOICE_DATE --  ST_DOCUMENT_INFO - MAX(CREATED_ON)
    ,pa.CLOSED_DATE ACCOUNT_CLOSE_DATE
    ,NULL CORRESPONDENCE_DEL_MODE
    ,'ENGLISH' LANGUAGE_PREFERENCE
    ,'OPT-OUT' MOBILE_ALERTS
    ,NULL CHALLENGE_QUESTION    --KS_CHALLENGE_QUESTION.QUESTION   KS_USER_CHALLENGE_RESPONSE?
    ,NULL CHALLENGE_ANSWER -- KS_USER_CHALLENGE_RESPONSE.ANSWER
    ,NULL IS_POSTPAID  -- Derived
    ,NULL IS_SURVEY_OPTED  -- N/A
    ,NULL ACC_LOCKED_DATETIME -- N/A
    ,NULL ACCOUNT_LOCKED -- N/A
    ,NULL NSF_LOCK_RELEASED_DT -- N/A
    ,NULL CHARGEBACK_RELEASED_DT -- N/A
    ,pad.BKTY_FILE_DATE BANKRUPTCY
    ,'N' COURT_FLAG
    ,NULL OUTBOUND_DAILER
    ,NULL DO_NOT_CALL
    ,NULL APPLICATION_NUMBER
    ,pad.CA_AGENCY_ID COLLECTION_AGENCY -- Derived based on the Account number
    ,NULL ACCOUNT_CLOSE_PEND_DT
    ,pad.DECEASED_DATE DECEASED
    ,pa.CREATED_ON CREATED   -- SYSDATE ??
    ,'SUNTOLL_CSC_ID' CREATED_BY  -- SUNTOLL_CSC_ID
    ,to_date('02-27-2017', 'MM-DD-YYYY') LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY    -- SUNTOLL_CSC_ID
    ,pa.MAIDEN_NAME MOTHERS_MAIDEN_NAME
    ,pa.EIN FEIN_NUMBER
    ,pad.IS_CAST CAST_ACCOUNT_FLAG
    ,pa.TAX_EXEMPT_ID TAX_EXEMPTION_ID  -- TAX_EXEMPTION_ID
    ,pa.TAX_EXEMPT_EXPIRE_DATE EXPIRATION_DATE
    ,pa.ANON_ACCT ANONYMOUS_ACCOUNT
    ,NULL SECONDARY_EMAIL_ADDRESS
    ,'SUNTOLL' SOURCE_SYSTEM
--    ,pad.BKTY_FILE_DATE BKTY_FILE_DATE
--    ,pad.BKTY_DICHG_DATE BKTY_DICHG_DATE
--    ,pad.BKTY_DISMISS_DATE BKTY_DISMISS_DATE
--    ,pad.BKTY_CASEID BKTY_CASEID
--    ,pad.BKTY_NOTIFY_BY BKTY_NOTIFY_BY
--    ,pad.DECEASE_NOTIFY_BY DECEASE_NOTIFY_BY
--    ,pad.DEATH_CERT_DATE DEATH_CERT_DATE
--    ,pad.COLL_ASSIGN_DATE COLL_ASSIGN_DATE
--    ,pad.ACCT_HOLD_DATE INIT_COLL_ASSIGN_DATE
--    ,pad.ACCT_HOLD_DATE ACCT_HOLD_DATE
--    ,pad.IS_REGISTERED IS_REGISTERED
--    ,NULL OWNER_TYPE
----    ,pa.OWNER_TYPE OWNER_TYPE
---- IF ACCOUNT_NUM EXISTS IN ST_REG_STOP_PAYMENT_PLAN AND 
----  NVL(PAYMENT_PLAN_END, TRUNC(SYSDATE)) ST_REG_STOP_PAYMENT_PLAN) >=  TRUNC(SYSDATE) THEN 'A' 
----  (if plan end date is future or null) ELSE 'N'
--
----    ,case when pa.ACCT_NUM = (select ACCT_NUM 
----                            from PATRON.ST_REG_STOP_PAYMENT_PLAN rsp
----                            where rsp.ACCT_NUM = pa.ACCT_NUM
----                            and nvl(PAYMENT_PLAN_END, trunc(SYSDATE)) >= trunc(SYSDATE)) THEN 'A' 
----          else 'N'
----        end PAYMENT_PLAN
--    ,NULL PAYMENT_PLAN
    
    ,NULL ACTIVITY_ID
    ,NULL X_VERSION
    ,NULL EMP_NUM
    ,NULL X_POSTPAIDSTATUS
    ,NULL X_POST_PAID_STS_INT_ID
    ,NULL X_ACT_OPEN_MODE
    ,NULL X_APPLICATION_RECEIPTDATETIME
    ,NULL X_BANKRUPTCY_FLG
    ,NULL X_BLOCK_END_DT
    ,NULL X_BLOCK_ESCALATION
    ,NULL X_BLOCK_START_DT
    ,NULL X_CCU_ENDDATE
    ,NULL X_CCU_STARTDATE
    ,NULL X_CLOSURE_REASON
    ,NULL X_COLLATERALTYPE
    ,NULL X_COLLECTION_STATUS
    ,NULL X_COMMENTS
    ,NULL X_COMMERCIAL_AXLE_COUNT
    ,NULL X_DEVICEESTIMATEDCOLLATERAL
    ,NULL X_EXCLUDECOLLECTION
    ,NULL X_EXCLUDEREVOCATION
    ,NULL X_INVOICECYCLESTARTDATE
    ,NULL X_INVOICEDELIVERYFREQUENCYMODE
    ,NULL X_INVOICE_DELIVERY_MODE
    ,NULL X_ISDEVICEDEPOSIT
    ,NULL X_MARKETINGCODE
    ,NULL X_PAY_PLAN_FLG
    ,NULL X_PRIMARYORGANIZATION_ID
    ,NULL X_PRIMARY_ADDRESS_ID
    ,NULL X_PRIMARY_CONTACT_ID
    ,NULL X_PRIMARY_REBILL_ID
    ,NULL X_REPLENISHMENTCUSTOMERAMOUNT
    ,NULL X_REPLENISHMENTQEVALAMOUNT
    ,NULL X_REPLENISHMENTTHRESHOLDAMOUNT
    ,NULL X_REVOCATIONFINALBALANCE
    ,NULL X_SPEED_STATUS_ID
    ,NULL X_SWAPRETURNFEEEXCEMPT
    ,NULL X_TAXEXEMPT
    ,NULL X_TAXILEMO
    ,NULL X_TAXILIMOASSOCIATIONNAME
    ,NULL X_THRUWAY_REF_NUMBER
FROM PA_ACCT pa
    ,PA_ACCT_DETAIL pad
--    ,PATRON.KS_CHALLENGE_QUESTION
WHERE pa.ACCT_NUM = pad.ACCT_NUM --(+)
--AND   paa.ACCT_NUM >= p_begin_acct_num AND   paa.ACCT_NUM <= p_end_acct_num
;
/*Change FTE_TABLE to the actual table name*/

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
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_HST_INFO_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    FOR i IN 1 .. DM_ACCOUNT_HST_INFO_tab.COUNT LOOP

      begin
        select max(sc.STATUS_CHG_DATE) into  DM_ACCOUNT_HST_INFO_tab(i).ACCOUNT_STATUS_DATETIME
        from PA_ACCT_STATUS_CHANGES sc
        where sc.ACCT_NUM = DM_ACCOUNT_HST_INFO_tab(i).ACCOUNT_NUMBER
        ;
      exception 
        when others then null;
        DM_ACCOUNT_HST_INFO_tab(i).ACCOUNT_STATUS_DATETIME:=null;
      end;
      begin
        select max(di.CREATED_ON) into  DM_ACCOUNT_HST_INFO_tab(i).LAST_INVOICE_DATE
        from ST_DOCUMENT_INFO di
        where di.ACCT_NUM = DM_ACCOUNT_HST_INFO_tab(i).ACCOUNT_NUMBER
        ;
      exception 
        when others then null;
        DM_ACCOUNT_HST_INFO_tab(i).LAST_INVOICE_DATE:=null;
      end;
--    DBMS_OUTPUT.PUT_LINE('ACCT- '||i||' - '||DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
--        ||' - '||DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS_DATETIME
--        ||' - '||DM_ACCOUNT_INFO_tab(i).LAST_INVOICE_DATE);
           
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
    FORALL i in DM_ACCOUNT_HST_INFO_tab.first .. DM_ACCOUNT_HST_INFO_tab.last
           INSERT INTO DM_ACCOUNT_HST_INFO VALUES DM_ACCOUNT_HST_INFO_tab(i);
                       
    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    update_track_proc(v_trac_etl_rec);
    COMMIT;
    
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('Total ROW_CNT : '||ROW_CNT);
  
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


