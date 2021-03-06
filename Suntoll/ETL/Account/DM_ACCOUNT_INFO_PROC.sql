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

/*
    ,decode(pa.ACCTSTAT_ACCT_STATUS_CODE,
     '01','ACTIVE',       --  01	Active           
     '02','SUSPENDED',    --  02	Suspended   
     '03','RVKF',         --  03	Terminated
     '04','CLOSED',       --  04	Closed
     '05','Never Activate', --  05	Never Activate
     '06','Pending',      --  06	Pending
     '07','Pre Paid',     --  07	Pre Paid
     '08','Post Paid',    --  08  Post Paid
     '09','Bankruptcy',   --  09	Bankruptcy 
     '99','DO NOT USE',   --  99	Do Not Use
    pa.ACCTSTAT_ACCT_STATUS_CODE||'NO-MAPPING'
    ) ACCOUNT_STATUS

Account STATUS

VECTOR ACCOUNT TYPE	Legacy	 
ACTIVE	01 from FL gets mapped to ACTIVE	 
RVKF	  03 (Terminated) from FL gets mapped to RVKF)	 
CLOSED	04 (CLOSED) from FL gets mapped to CLOSED	 

  06 (PENDING) - > Set it to CLOSEPENDING if current balance is 0
  06 (PENDING) - > Set it to ACTIVE if current balance is > 0
  07 (Pre paid) and 8 (Post Paid) -> set it to ACTIVE
*/

CURSOR C1
(p_begin_acct_num  pa_acct.acct_num%TYPE, p_end_acct_num    pa_acct.acct_num%TYPE)
IS SELECT 
    pa.ACCT_NUM ACCOUNT_NUMBER
    ,decode(pa.ACCTSTAT_ACCT_STATUS_CODE,
     '01','ACTIVE',
     '02','SUSPENDED',
     '03','RVKF',
     '04','CLOSED',
     '06','CLOSEPENDING',
     '07','ACTIVE',
     '08','ACTIVE',
     '99','DO NOT USE',
    pa.ACCTSTAT_ACCT_STATUS_CODE||'NO-MAPPING'
    ) ACCOUNT_STATUS
    ,NULL ACCOUNT_STATUS_DATETIME  -- in ETL- Sub query  max(PA_ACCT_STATUS_CHANGES.STATUS_CHG_DATE)
--    If PA_ACCT.E_MAIL_ADDR IS NOT NULL then 'EMAIL' ELSE 'MAIL'
    ,nvl2(pa.E_MAIL_ADDR, 'EMAIL', 'MAIL') STATEMENT_DELIVERY_MODE
    ,'NONE' STATEMENT_PERIOD    -- PA_ACCT.Statement_option decode? MONTHLY? STATOPTCDE_STATEMENT_OPT_CODE
    ,decode(pa.ACCTTYPE_ACCT_TYPE_CODE,
            '01','PRIVATE',
            '02','BUSINESS',
            '03','PVIDEOREG',
            '04','PVIDEOUNREG',
            '05','SHORTTERMVIDEO',
            '06','BVIDEOREG',
            '07','BVIDEOUNREG',
            '08','PVIOLATOR',
            '09','CVIOLATOR',
            pa.ACCTTYPE_ACCT_TYPE_CODE||'-NOMAPPING') ACCOUNT_TYPE 
    ,pa.CREATED_ON ACCOUNT_OPEN_DATE  
    ,trim(pa.L_NAME)||', '||trim(pa.F_NAME) ACCOUNT_NAME
    ,nvl(pa.ORG,'DM_UNKNOWN') COMPANY_NAME
    ,NULL DBA
    
    ,pa.E_MAIL_ADDR EMAIL_ADDRESS
-- IN ETL   ,PA_ACCT_REPL_DETAIL.REPLENISHMENT_AMT REBILL_AMOUNT  
    ,0 REBILL_AMOUNT 
-- IN ETL   ,PA_ACCT_TRANSP.LOW_BAL_AMT REBILL_THRESHOLD  
    ,0 REBILL_THRESHOLD 
    ,pa.ACCT_PIN_NUMBER PIN
    ,0 VIDEO_ACCT_STATUS
    ,NULL SUSPENDED_DATE
    ,NULL LAST_INVOICE_DATE  -- in ETL - max(ST_DOCUMENT_INFO.CREATED_ON)
    ,pa.CLOSED_DATE ACCOUNT_CLOSE_DATE
    ,NULL CORRESPONDENCE_DEL_MODE   -- Derived ?
    ,'ENGLISH' LANGUAGE_PREFERENCE
    ,'OPT-OUT' MOBILE_ALERTS
    ,'What is the PIN Number?' CHALLENGE_QUESTION 
    ,pa.ACCT_PIN_NUMBER CHALLENGE_ANSWER 
    ,NULL IS_POSTPAID
    ,NULL IS_SURVEY_OPTED
    ,NULL ACC_LOCKED_DATETIME
    ,NULL ACCOUNT_LOCKED
    ,NULL NSF_LOCK_RELEASED_DT
    ,NULL CHARGEBACK_RELEASED_DT
    ,pad.BKTY_FILE_DATE BANKRUPTCY_DATE  
    ,'N' COURT_FLAG
    ,NULL OUTBOUND_DAILER
    ,NULL DO_NOT_CALL
    ,NULL APPLICATION_NUMBER
    ,pad.CA_AGENCY_ID COLLECTION_AGENCY  -- Derived based on the Account number
    ,NULL ACCOUNT_CLOSE_PEND_DT
    ,pad.DECEASED_DATE DECEASED
    ,pa.CREATED_ON CREATED 
    ,'SUNTOLL_CSC_ID' CREATED_BY 
    ,to_date('02-27-2017', 'MM-DD-YYYY') LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY 
    ,pa.MAIDEN_NAME MOTHERS_MAIDEN_NAME
    ,pa.EIN FEIN_NUMBER
    ,pad.IS_CAST CAST_ACCOUNT_FLAG
    ,pa.TAX_EXEMPT_ID TAX_EXEMPTION_ID 
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
    ,NULL OWNER_TYPE  -- source table?
    ,pad.DEATH_CERT_DATE DEATH_CERT_DATE
    ,pad.COLL_ASSIGN_DATE COLL_ASSIGN_DATE
    ,pad.DECEASE_NOTIFY_BY DECEASE_NOTIFY_BY
    ,pad.INIT_COLL_ASSIGN_DATE INIT_COLL_ASSIGN_DATE
--    ,NULL PAYMENT_PLAN  -----------------------------------------
--IF ACCOUNT_NUM EXISTS IN ST_REG_STOP_PAYMENT_PLAN AND 
--NVL(PAYMENT_PLAN_END, TRUNC(SYSDATE))  >=  TRUNC(SYSDATE) THEN 'A' 
--(if plan end date is future or null) ELSE 'N'
FROM PA_ACCT pa
    ,PA_ACCT_DETAIL pad
WHERE  pa.ACCT_NUM = pad.ACCT_NUM (+)
AND   pa.ACCT_NUM >= p_begin_acct_num AND   pa.ACCT_NUM <= p_end_acct_num
and   pa.ACCT_NUM>0
; 

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

  OPEN C1(v_trac_rec.begin_acct,v_trac_rec.end_acct);  
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

--  6 (PENDING) - > Set it to CLOSEPENDING if current balance is 0
--  6 (PENDING) - > Set it to ACTIVE if current balance is > 0
-- Read ACCT_STATUS_CODE from PA_ACCT and 
-- join PA_ACCT to KS_ACCT_LEDGER on ACCT_NUM for balance     
      IF DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS = 'CLOSEPENDING' then
        begin
          select  case when BALANCE > 0 then 'ACTIVE' 
                       when BALANCE < 0 then 'NEG BALANCE'
                       else DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS
                  end
          into    DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS
          from    KS_LEDGER -- k1
          where   ACCT_NUM=DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
  --        and     rownum<=1
          and     ID = (select max(ID) 
                        from    KS_LEDGER
                        where   ACCT_NUM=DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER)
  --        and     POSTED_DATE = (select max(POSTED_DATE) 
  --                              from    KS_LEDGER -- k2
  --                              where   PA_LANE_TXN_ID=DM_VIOL_TX_EVENT_INFO_tab(i).LANE_TX_ID)
          ;
        exception 
        when others then null;   
            v_trac_etl_rec.result_code := SQLCODE;
            v_trac_etl_rec.result_msg := SQLERRM;
            v_trac_etl_rec.proc_end_date := SYSDATE;
            update_track_proc(v_trac_etl_rec);
            DBMS_OUTPUT.PUT_LINE('REB ERROR CODE: '||v_trac_etl_rec.result_code);
            DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
        end;

      end if;


      begin
        select max(STATUS_CHG_DATE)
        into DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS_DATETIME
        from PA_ACCT_STATUS_CHANGES
        where ACCT_NUM = DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
        ;
      exception 
        when others then null;
        DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS_DATETIME:=null;
      end;

--    ,PA_ACCT_REPL_DETAIL.REPLENISHMENT_AMT REBILL_AMOUNT  -- If many sum or last
      begin
        select REPLENISHMENT_AMT 
        into DM_ACCOUNT_INFO_tab(i).REBILL_AMOUNT
        from PA_ACCT_REPL_DETAIL
        where ACCT_ACCT_NUM = DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
        ;
      exception 
        when no_data_found then
          DM_ACCOUNT_INFO_tab(i).REBILL_AMOUNT:=0;       
        when others then
          DM_ACCOUNT_INFO_tab(i).REBILL_AMOUNT:=0;
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('REB ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;

--    ,PA_ACCT_TRANSP.LOW_BAL_AMT REBILL_THRESHOLD     -- If many sum or last
      begin
        select LOW_BAL_AMT 
        into DM_ACCOUNT_INFO_tab(i).REBILL_THRESHOLD
        from PA_ACCT_TRANSP
        where ACCT_ACCT_NUM = DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
        ;
      exception 
        when no_data_found then
          DM_ACCOUNT_INFO_tab(i).REBILL_THRESHOLD:=0;
        when others then
          v_trac_etl_rec.result_code := SQLCODE;
          v_trac_etl_rec.result_msg := SQLERRM;
          v_trac_etl_rec.proc_end_date := SYSDATE;
          update_track_proc(v_trac_etl_rec);
          DBMS_OUTPUT.PUT_LINE('LOW ERROR CODE: '||v_trac_etl_rec.result_code);
          DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||v_trac_etl_rec.result_msg);
      end;

--COLLECTION_AGENCY =  Convert the Number into a description
--from RETAILER_PARTNERS table; ask David to copy for you.  
--Source PA_ACCT_DETAIL.CA_AGENCY_ID = RETAILER_PARTNERS.ACCT_NUM
      begin
        select PARTNER_NAME
        into  DM_ACCOUNT_INFO_tab(i).COLLECTION_AGENCY
        from  RETAIL_PARTNERS
        where ACCT_NUM = DM_ACCOUNT_INFO_tab(i).COLLECTION_AGENCY -- PA_ACCT_DETAIL.CA_AGENCY_ID
        ;
      exception 
        when others then null;
      end;
      
      begin
        select max(CREATED_ON) 
        into  DM_ACCOUNT_INFO_tab(i).LAST_INVOICE_DATE
        from ST_DOCUMENT_INFO
        where ACCT_NUM = DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
        ;
      exception 
        when others then null;
        DM_ACCOUNT_INFO_tab(i).LAST_INVOICE_DATE:=null;
      end;
           
--    IF ACCOUNT_NUM EXISTS IN ST_REG_STOP_PAYMENT_PLAN AND NVL(PAYMENT_PLAN_END, TRUNC(SYSDATE))  >=  TRUNC(SYSDATE) 
--    THEN 'A'    -- (if plan end date is future or null) 
--    ELSE 'N' 

      v_trac_etl_rec.track_last_val := DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER;
    END LOOP;
--      ETL SECTION END

    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_INFO_tab.first .. DM_ACCOUNT_INFO_tab.last
           INSERT INTO DM_ACCOUNT_INFO VALUES DM_ACCOUNT_INFO_tab(i);

    row_cnt := row_cnt +  SQL%ROWCOUNT;
    v_trac_etl_rec.dm_load_cnt := row_cnt;
    v_trac_etl_rec.end_val := v_trac_etl_rec.track_last_val;
    update_track_proc(v_trac_etl_rec);
    COMMIT;
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Total Load count : '||ROW_CNT);

  CLOSE C1;

  COMMIT;
  v_trac_etl_rec.status := 'ETL Completed';
  v_trac_etl_rec.result_code := SQLCODE;
  v_trac_etl_rec.result_msg := SQLERRM;
  v_trac_etl_rec.end_val := v_trac_rec.end_acct;
  v_trac_etl_rec.proc_end_date := SYSDATE;
  update_track_proc(v_trac_etl_rec);
  DBMS_OUTPUT.PUT_LINE('END '||v_trac_etl_rec.etl_name||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  
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

commit;

