/********************************************************
*
* Name: DM_ACCOUNT_INFO_PROC
* Created by: RH, 5/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCOUNT_INFO
*
********************************************************/
--Source field different from target as mention in worksheet 
--by previous developer. Detail needed. No action taken
set serveroutput on
set verify on
set echo on

--declare
CREATE OR REPLACE PROCEDURE DM_ACCOUNT_INFO_PROC   
  (i_trac_id dm_tracking_etl.track_etl_id%TYPE)
IS

TYPE DM_ACCOUNT_INFO_TYP IS TABLE OF DM_ACCOUNT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCOUNT_INFO_tab DM_ACCOUNT_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1
--(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    pa.ACCT_NUM ACCOUNT_NUMBER
    ,pa.ACCTSTAT_ACCT_STATUS_CODE ACCOUNT_STATUS
    ,NULL ACCOUNT_STATUS_DATETIME    --  Sub query  ,max(PA_ACCT_STATUS_CHANGES.STATUS_CHG_DATE)  
    ,'MAIL' STATEMENT_DELIVERY_MODE 
    ,'NONE' STATEMENT_PERIOD    -- PA_ACCT.Statement_option decode? MONTHLY? STATOPTCDE_STATEMENT_OPT_CODE
    ,pa.ACCTTYPE_ACCT_TYPE_CODE ACCOUNT_TYPE
    ,pa.CREATED_ON ACCOUNT_OPEN_DATE    -- ACCT_OPEN_DATE 
    ,trim(pa.L_NAME)||', '||trim(pa.F_NAME) ACCOUNT_NAME
    ,pa.ORG COMPANY_NAME
    ,NULL DBA
    ,pa.E_MAIL_ADDR EMAIL_ADDRESS
--    ,PA_ACCT_REPL_DETAIL.REPLENISHMENT_AMT REBILL_AMOUNT  -- If many sum or last
    ,0 REBILL_AMOUNT  -- If many sum or last
--    ,PA_ACCT_REPL_DETAIL.LOW_BAL_AMT REBILL_THRESHOLD     -- If many sum or last
    ,0 REBILL_THRESHOLD     -- If many sum or last    ,0 PIN
    ,pa.ACCT_PIN_NUMBER PIN
    ,0 VIDEO_ACCT_STATUS
    ,NULL SUSPENDED_DATE
    ,NULL LAST_INVOICE_DATE  -- Sub query   ,max(ST_DOCUMENT_INFO.CREATED_ON)
--    ,NULL ACCOUNT_CLOSE_DATE
    ,pa.CLOSED_DATE ACCOUNT_CLOSE_DATE
    ,NULL CORRESPONDENCE_DEL_MODE   -- Derived ?
    ,'ENGLISH' LANGUAGE_PREFERENCE
    ,'OPT-OUT' MOBILE_ALERTS
    ,NULL CHALLENGE_QUESTION  -- KS_USER_CHALLENGE_RESPONSE.QUESTION
    ,NULL CHALLENGE_ANSWER  -- KS_USER_CHALLENGE_RESPONSE.ANSWER - KEY
    ,NULL IS_POSTPAID
    ,NULL IS_SURVEY_OPTED
    ,NULL ACC_LOCKED_DATETIME
    ,NULL ACCOUNT_LOCKED
    ,NULL NSF_LOCK_RELEASED_DT
    ,NULL CHARGEBACK_RELEASED_DT
    ,pad.BKTY_FILE_DATE BANKRUPTCY_DATE  -- 
    ,'N' COURT_FLAG
    ,NULL OUTBOUND_DAILER
    ,NULL DO_NOT_CALL
    ,NULL APPLICATION_NUMBER
    ,pad.CA_AGENCY_ID COLLECTION_AGENCY  -- Derived based on the Account number
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
    ,pad.BKTY_FILE_DATE BKTY_FILE_DATE
    ,pad.BKTY_DICHG_DATE BKTY_DICHG_DATE
    ,pad.BKTY_DISMISS_DATE BKTY_DISMISS_DATE
    ,pad.BKTY_CASEID BKTY_CASEID
    ,pad.BKTY_NOTIFY_BY BKTY_NOTIFY_BY
    ,pad.IS_REGISTERED IS_REGISTERED
    ,NULL OWNER_TYPE
    ,NULL DEATH_CERT_DATE
    ,NULL COLL_ASSIGN_DATE
    ,NULL DECEASE_NOTIFY_BY
    ,NULL INIT_COLL_ASSIGN_DATE
--    ,NULL PAYMENT_PLAN  -----------------------------------------
--IF ACCOUNT_NUM EXISTS IN ST_REG_STOP_PAYMENT_PLAN AND 
--NVL(PAYMENT_PLAN_END, TRUNC(SYSDATE))  >=  TRUNC(SYSDATE) THEN 'A' 
--(if plan end date is future or null) ELSE 'N'
FROM PA_ACCT pa
    ,PA_ACCT_DETAIL pad
WHERE  pa.ACCT_NUM = pad.ACCT_NUM (+)
--AND pa.ACCT_NUM > 0
--and rownum<3501
--AND   pa.ACCT_NUM >= p_begin_acct_num AND   pa.ACCT_NUM <= p_end_acct_num
; 

SQL_STRING  varchar2(500) := 'delete table ';

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

  OPEN C1; --(v_trac_rec.begin_acct,v_trac_rec.end_acct);  
  v_trac_etl_rec.status := 'ETL Processing ';
  update_track_proc(v_trac_etl_rec);

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_INFO_tab
    LIMIT P_ARRAY_SIZE --LIMIT 1000
    ;
    
--ETL SECTION BEGIN mapping
-- DBMS_OUTPUT.PUT_LINE('FETCH '||LOAD_TAB||' ETL at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

    FOR i IN 1 .. DM_ACCOUNT_INFO_tab.COUNT LOOP
      IF i=1 then
        v_trac_etl_rec.BEGIN_VAL := DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER;
      end if;
      
      begin
        select max(sc.STATUS_CHG_DATE) into  DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS_DATETIME
        from PA_ACCT_STATUS_CHANGES sc
        where sc.ACCT_NUM = DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
        ;
      exception 
        when others then null;
        DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS_DATETIME:=null;
      end;
      
      begin
        select max(di.CREATED_ON) into  DM_ACCOUNT_INFO_tab(i).LAST_INVOICE_DATE
        from ST_DOCUMENT_INFO di
        where di.ACCT_NUM = DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
        ;
      exception 
        when others then null;
        DM_ACCOUNT_INFO_tab(i).LAST_INVOICE_DATE:=null;
      end;
--    DBMS_OUTPUT.PUT_LINE('ACCT- '||i||' - '||DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
--        ||' - '||DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS_DATETIME
--        ||' - '||DM_ACCOUNT_INFO_tab(i).LAST_INVOICE_DATE);
           
--    IF ACCOUNT_NUM EXISTS IN ST_REG_STOP_PAYMENT_PLAN AND NVL(PAYMENT_PLAN_END, TRUNC(SYSDATE))  >=  TRUNC(SYSDATE) 
--    THEN 'A'    -- (if plan end date is future or null) 
--    ELSE 'N' 

      v_trac_etl_rec.track_last_val := DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER;
      v_trac_etl_rec.end_val := DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER;

    END LOOP;
--      ETL SECTION END

    /*Bulk insert int Acount drive table list */    
--    FORALL i in DM_ACCOUNT_INFO_tab.first .. DM_ACCOUNT_INFO_tab.last
--           INSERT INTO DM_ACCOUNT_LIST VALUES DM_ACCOUNT_INFO_tab(i).ACCT_NUM;    

    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_INFO_tab.first .. DM_ACCOUNT_INFO_tab.last
           INSERT INTO DM_ACCOUNT_INFO VALUES DM_ACCOUNT_INFO_tab(i);

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


