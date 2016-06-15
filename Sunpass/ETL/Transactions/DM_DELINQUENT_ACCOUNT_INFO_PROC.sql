/********************************************************
*
* Name: DM_DELINQUENT_ACCOUNT_I_PROC
* Created by: DT, 5/2/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_DELINQUENT_ACCOUNT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_DELINQUENT_ACCOUNT_I_PROC IS

TYPE DM_DELINQUENT_ACCOUNT_I_TYP IS TABLE OF DM_DELINQUENT_ACCOUNT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_DELINQUENT_ACCOUNT_I_tab DM_DELINQUENT_ACCOUNT_I_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL X_COLL_STATUS
    ,NULL X_LAST_UPD_COLL_BALANCE
    ,NULL X_LAST_UPD_COLL_DATETIME
    ,NULL X_ORIG_COLL_BALANCE
    ,NULL X_ORIG_COLL_DATETIME
    ,ACCT_NUM X_ETC_ACCNT_ID
    ,NULL X_REVOKE_BALANCE
    ,NULL X_RVKD_FINL_DT
    ,'0' X_RVK_TAGDP_FRFIT
    ,'0' X_RVK_TAGLST_FEE
    ,'0' X_REVOKE_FEE
    ,NULL X_RVKD_WRN_DT
    ,'F' X_COLL_EXCLUDE_FLG
    ,'0' X_COLL_SETTLE_BALANCE
    ,to_date('01/01/1900','MM/DD/YYYY') X_COLL_SETTLE_DATETIME
    ,'0' X_COLL_SETTLE_DISMISSAL
    ,'0' X_COLL_SETTLE_PAYMENT
    ,'0' X_COLL_SETTLE_UNCOLLECTABLE
    ,ACCT_NUM ISSUE_NUM
    ,to_date('01/01/1900','MM/DD/YYYY') X_GD_STATUS_DT
    ,'0' X_NBL_LTTR_CNT
    ,to_date('01/01/1900','MM/DD/YYYY') X_NBL_LTTR_DT
    ,NULL X_REVOKE_EXCLUDE
    ,NULL X_WRN_LTTR_CNT
    ,NULL X_ZRO_STATUS_DT
    ,NULL ACCNT_STATUS
    ,LAST_LETTER_DATE UPDATE_TS
    ,'0' X_COLL_LAST_DISP_CODE
    ,'F' WRITE_OFF_FLAG
    ,to_date('01/01/1900','MM/DD/YYYY') PROCESSING_DATE
    ,'0' WOFF_REJECT_REASON
    ,to_date('01/01/1900','MM/DD/YYYY') ACTUAL_WRITEOFF_DATE
    ,'0' FINAL_WOFF_AMOUNT
    ,'SUNPASS' SOURCE_SYSTEM
FROM ST_ACCT_SUSPENSE_WATCH;

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_DELINQUENT_ACCOUNT_I_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_DELINQUENT_ACCOUNT_I_tab.first .. DM_DELINQUENT_ACCOUNT_I_tab.last
           INSERT INTO DM_DELINQUENT_ACCOUNT_INFO VALUES DM_DELINQUENT_ACCOUNT_I_tab(i);
                       
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


