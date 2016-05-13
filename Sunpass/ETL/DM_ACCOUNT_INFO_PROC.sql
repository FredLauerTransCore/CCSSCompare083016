/********************************************************
*
* Name: DM_ACCOUNT_INFO_PROC
* Created by: DT, 4/13/2016
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
    NULL ACCOUNT_NUMBER
    ,NULL ACCOUNT_STATUS
    ,NULL ACCOUNT_STATUS_DATETIME
    ,NULL STATEMENT_DELIVERY_MODE
    ,NULL STATEMENT_PERIOD
    ,NULL ACCOUNT_TYPE
    ,NULL ACCOUNT_OPEN_DATE
    ,NULL ACCOUNT_NAME
    ,NULL COMPANY_NAME
    ,NULL DBA
    ,NULL EMAIL_ADDRESS
    ,NULL REBILL_AMOUNT
    ,NULL REBILL_THRESHOLD
    ,NULL PIN
    ,NULL VIDEO_ACCT_STATUS
    ,NULL SUSPENDED_DATE
    ,NULL LAST_INVOICE_DATE
    ,NULL ACCOUNT_CLOSE_DATE
    ,NULL CORRESPONDENCE_DEL_MODE
    ,NULL LANGUAGE_PREFERENCE
    ,NULL MOBILE_ALERTS
    ,NULL CHALLENGE_QUESTION
    ,NULL CHALLENGE_ANSWER
    ,NULL IS_POSTPAID
    ,NULL IS_SURVEY_OPTED
    ,NULL ACC_LOCKED_DATETIME
    ,NULL ACCOUNT_LOCKED
    ,NULL NSF_LOCK_RELEASED_DT
    ,NULL CHARGEBACK_RELEASED_DT
    ,NULL BANKRUPTCY_DATE
    ,NULL COURT_FLAG
    ,NULL OUTBOUND_DAILER
    ,NULL DO_NOT_CALL
    ,NULL APPLICATION_NUMBER
    ,NULL COLLECTION_AGENCY
    ,NULL ACCOUNT_CLOSE_PEND_DT
    ,NULL DECEASED
    ,NULL CREATED
    ,NULL CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,NULL MOTHERS_MAIDEN_NAME
    ,NULL FEIN_NUMBER
    ,NULL CAST_ACCOUNT_FLAG
    ,NULL TAX_EXEMPTION_ID
    ,NULL EXPIRATION_DATE
    ,NULL ANONYMOUS_ACCOUNT
    ,NULL SECONDARY_EMAIL_ADDRESS
    ,NULL SOURCE_SYSTEM
    ,NULL BKTY_FILE_DATE
    ,NULL BKTY_DICHG_DATE
    ,NULL BKTY_DISMISS_DATE
    ,NULL BKTY_CASEID
    ,NULL BKTY_NOTIFY_BY
    ,NULL IS_REGISTERED
    ,NULL OWNER_TYPE
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_ACCOUNT_INFO_tab
    LIMIT P_ARRAY_SIZE;
    EXIT WHEN C1%NOTFOUND;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_ACCOUNT_INFO_tab.first .. DM_ACCOUNT_INFO_tab.last
           INSERT INTO DM_ACCOUNT_INFO VALUES DM_ACCOUNT_INFO_tab(i);
                       

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

