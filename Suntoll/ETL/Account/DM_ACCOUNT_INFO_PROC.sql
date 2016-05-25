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

CREATE OR REPLACE PROCEDURE DM_ACCOUNT_INFO_PROC IS

TYPE DM_ACCOUNT_INFO_TYP IS TABLE OF DM_ACCOUNT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_ACCOUNT_INFO_tab DM_ACCOUNT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    PA_ACCT.ACCT_NUM ACCOUNT_NUMBER
    ,PA_ACCT.ACCTSTAT_ACCT_STATUS_CODE ACCOUNT_STATUS
    ,NULL ACCOUNT_STATUS_DATETIME    --  Sub query  ,max(PA_ACCT_STATUS_CHANGES.STATUS_CHG_DATE)  
    ,NULL STATEMENT_DELIVERY_MODE
    ,STATOPTCDE_STATEMENT_OPT_CODE STATEMENT_PERIOD    -- PA_ACCT.Statement_option decode? MONTHLY?
    ,PA_ACCT.ACCTTYPE_ACCT_TYPE_CODE ACCOUNT_TYPE
    ,PA_ACCT.CREATED_ON ACCOUNT_OPEN_DATE    -- ACCT_OPEN_DATE 
    ,PA_ACCT.L_NAME||', '||PA_ACCT.F_NAME ACCOUNT_NAME
    ,PA_ACCT.ORG COMPANY_NAME
    ,NULL DBA
    ,PA_ACCT.E_MAIL_ADDR EMAIL_ADDRESS
--    ,PA_ACCT_REPL_DETAIL.REPLENISHMENT_AMT REBILL_AMOUNT  -- If many sum or last
--    ,PA_ACCT_REPL_DETAIL.LOW_BAL_AMT REBILL_THRESHOLD     -- If many sum or last
    ,PA_ACCT.ACCT_PIN_NUMBER PIN
    ,NULL VIDEO_ACCT_STATUS
    ,NULL SUSPENDED_DATE
    ,NULL LAST_INVOICE_DATE  -- Sub query   ,max(ST_DOCUMENT_INFO.CREATED_ON) LAST_INVOICE_DATE 
    ,PA_ACCT.CLOSED_DATE ACCOUNT_CLOSE_DATE
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
    ,PA_ACCT_DETAIL.BKTY_FILE_DATE BANKRUPTCY_DATE  -- 
    ,'NO' COURT_FLAG
    ,NULL OUTBOUND_DAILER
    ,NULL DO_NOT_CALL
    ,NULL APPLICATION_NUMBER
    ,PA_ACCT_DETAIL.CA_AGENCY_ID COLLECTION_AGENCY  -- Derived based on the Account number
    ,NULL ACCOUNT_CLOSE_PEND_DT
    ,PA_ACCT_DETAIL.DECEASED_DATE DECEASED
    ,PA_ACCT.CREATED_ON CREATED   -- SYSDATE ??
    ,'SUNTOLL_CSC_ID' CREATED_BY  -- SUNTOLL_CSC_ID
    ,to_date('02-27-2017', 'MM-DD-YYYY') LAST_UPD
    ,'SUNTOLL_CSC_ID' LAST_UPD_BY    -- SUNTOLL_CSC_ID
    ,PA_ACCT.MAIDEN_NAME MOTHERS_MAIDEN_NAME
    ,PA_ACCT.EIN FEIN_NUMBER
    ,PA_ACCT_DETAIL.IS_CAST CAST_ACCOUNT_FLAG
    ,PA_ACCT.TAX_EXEMPT_ID TAX_EXEMPTION_ID  -- TAX_EXEMPTION_ID
    ,PA_ACCT.TAX_EXEMPT_EXPIRE_DATE EXPIRATION_DATE
    ,PA_ACCT.ANON_ACCT ANONYMOUS_ACCOUNT
    ,NULL SECONDARY_EMAIL_ADDRESS
    ,'SUNTOLL' SOURCE_SYSTEM
    ,PA_ACCT_DETAIL.BKTY_FILE_DATE BKTY_FILE_DATE
    ,PA_ACCT_DETAIL.BKTY_DICHG_DATE BKTY_DICHG_DATE
    ,PA_ACCT_DETAIL.BKTY_DISMISS_DATE BKTY_DISMISS_DATE
    ,PA_ACCT_DETAIL.BKTY_CASEID BKTY_CASEID
    ,PA_ACCT_DETAIL.BKTY_NOTIFY_BY BKTY_NOTIFY_BY
    ,PA_ACCT_DETAIL.IS_REGISTERED IS_REGISTERED
    ,NULL OWNER_TYPE
--    ,NULL PAYMENT_PLAN  -----------------------------------------
--IF ACCOUNT_NUM EXISTS IN ST_REG_STOP_PAYMENT_PLAN AND 
--NVL(PAYMENT_PLAN_END, TRUNC(SYSDATE))  >=  TRUNC(SYSDATE) THEN 'A' 
--(if plan end date is future or null) ELSE 'N'
FROM PATRON.PA_ACCT
    ,PATRON.PA_ACCT_DETAIL
--    ,PATRON.KS_CHALLENGE_QUESTION
WHERE PA_ACCT.ACCT_NUM = PA_ACCT_DETAIL.ACCT_NUM --(+)
--AND PA_ACCT.USER_ID = KS_USER_CHALLENGE_RESPONSE.USER_ID (+)
; 

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_INFO_tab
    LIMIT P_ARRAY_SIZE

--ETL SECTION BEGIN mapping
    FOR j IN 1 .. DM_ACCOUNT_INFO_tab.COUNT LOOP
     
      select max(sc.STATUS_CHG_DATE) into  DM_ACCOUNT_INFO_tab.ACCOUNT_STATUS_DATETIME(j)
      from PA_ACCT_STATUS_CHANGES sc
      where sc.ACCT_NUM = DM_ACCOUNT_INFO_tab.ACCOUNT_NUMBER(j)
      ;

      select max(di.CREATED_ON) into  DM_ACCOUNT_INFO_tab.LAST_INVOICE_DATE(j)
      from ST_DOCUMENT_INFO di
      where di.ACCT_NUM = DM_ACCOUNT_INFO_tab.ACCOUNT_NUMBER(j)
      ;
           
--    IF ACCOUNT_NUM EXISTS IN ST_REG_STOP_PAYMENT_PLAN AND NVL(PAYMENT_PLAN_END, TRUNC(SYSDATE))  >=  TRUNC(SYSDATE) 
--    THEN 'A'    -- (if plan end date is future or null) 
--    ELSE 'N' 

    END LOOP;
--      ETL SECTION END
    
    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_INFO_tab.first .. DM_ACCOUNT_INFO_tab.last
           INSERT INTO DM_ACCOUNT_INFO VALUES DM_ACCOUNT_INFO_tab(i);
                       
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


