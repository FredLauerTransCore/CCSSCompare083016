/********************************************************
*
* Name: DM_ACCOUNT_INFO_PROC
* Created by: RH, 5/18/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_ACCOUNT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

--CREATE OR REPLACE PROCEDURE DM_ACCOUNT_INFO_PROC IS
DECLARE

TYPE DM_ACCOUNT_INFO_TYP IS TABLE OF DM_ACCOUNT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCOUNT_INFO_tab DM_ACCOUNT_INFO_TYP;

P_ARRAY_SIZE NUMBER:=10000;

CURSOR C1 IS SELECT 
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
--    ,pad.BKTY_FILE_DATE BKTY_FILE_DATE
--    ,pad.BKTY_DICHG_DATE BKTY_DICHG_DATE
--    ,pad.BKTY_DISMISS_DATE BKTY_DISMISS_DATE
--    ,pad.BKTY_CASEID BKTY_CASEID
--    ,pad.BKTY_NOTIFY_BY BKTY_NOTIFY_BY
--    ,pad.IS_REGISTERED IS_REGISTERED
--    ,NULL OWNER_TYPE
--    ,NULL PAYMENT_PLAN  -----------------------------------------
--IF ACCOUNT_NUM EXISTS IN ST_REG_STOP_PAYMENT_PLAN AND 
--NVL(PAYMENT_PLAN_END, TRUNC(SYSDATE))  >=  TRUNC(SYSDATE) THEN 'A' 
--(if plan end date is future or null) ELSE 'N'
FROM PATRON.PA_ACCT pa
    ,PATRON.PA_ACCT_DETAIL pad
--    ,PATRON.KS_CHALLENGE_QUESTION
WHERE pa.ACCT_NUM = pad.ACCT_NUM --(+)
--AND PA_ACCT.USER_ID = KS_USER_CHALLENGE_RESPONSE.USER_ID (+)
and rownum<3501
; 

SQL_STRING  varchar2(500) := 'truncate table ';
LOAD_TAB    varchar2(50)  := 'DM_ACCOUNT_INFO';
ROW_CNT NUMBER := 0;

BEGIN
  DBMS_OUTPUT.PUT_LINE('Start '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  select count(1) into ROW_CNT from DM_ACCOUNT_INFO ;
  DBMS_OUTPUT.PUT_LINE('ROW_CNT : '||ROW_CNT);

  SQL_STRING := SQL_STRING||LOAD_TAB;
  DBMS_OUTPUT.PUT_LINE('SQL_STRING : '||SQL_STRING);
  execute immediate SQL_STRING;
  commit;
  select count(1) into ROW_CNT from DM_ACCOUNT_INFO ;
  DBMS_OUTPUT.PUT_LINE('ROW_CNT : '||ROW_CNT);
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_INFO_tab
--    LIMIT P_ARRAY_SIZE;
    LIMIT 1000;

--ETL SECTION BEGIN mapping
 DBMS_OUTPUT.PUT_LINE('FETCH '||LOAD_TAB||' ETL at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));

    FOR i IN 1 .. DM_ACCOUNT_INFO_tab.COUNT LOOP

      begin
        select max(sc.STATUS_CHG_DATE) into  DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS_DATETIME
        from PATRON.PA_ACCT_STATUS_CHANGES sc
        where sc.ACCT_NUM = DM_ACCOUNT_INFO_tab(i).ACCOUNT_NUMBER
        ;
      exception 
        when others then null;
        DM_ACCOUNT_INFO_tab(i).ACCOUNT_STATUS_DATETIME:=null;
      end;
      begin
        select max(di.CREATED_ON) into  DM_ACCOUNT_INFO_tab(i).LAST_INVOICE_DATE
        from PATRON.ST_DOCUMENT_INFO di
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

    END LOOP;
--      ETL SECTION END
    
    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_INFO_tab.first .. DM_ACCOUNT_INFO_tab.last
           INSERT INTO DM_ACCOUNT_INFO VALUES DM_ACCOUNT_INFO_tab(i);
--    DBMS_OUTPUT.PUT_LINE('Inserted '||sql%rowcount||' into '||LOAD_TAB||' at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
    ROW_CNT := ROW_CNT +  sql%rowcount;                 
    DBMS_OUTPUT.PUT_LINE('Total ROW count : '||ROW_CNT);
                       
    EXIT WHEN C1%NOTFOUND;
  END LOOP;

  COMMIT;

  CLOSE C1;

  COMMIT;
  DBMS_OUTPUT.PUT_LINE('END '||LOAD_TAB||' load at: '||to_char(SYSDATE,'MON-DD-YYYY HH:MM:SS'));
  DBMS_OUTPUT.PUT_LINE('ROW_CNT : '||ROW_CNT);
  
  EXCEPTION
  WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR CODE: '||SQLCODE);
     DBMS_OUTPUT.PUT_LINE('ERROR MSG: '||SQLERRM);
END;
--/
--SHOW ERRORS


