/********************************************************
*
* Name: DM_DELINQUENT_ACCOUNT_INFO_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_DELINQUENT_ACCOUNT_INFO
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_DELINQUENT_ACCOUNT_INFO_PROC IS

TYPE DM_DELINQUENT_ACCOUNT_INFO_TYP IS TABLE OF DM_DELINQUENT_ACCOUNT_INFO%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_DELINQUENT_ACCOUNT_INFO_tab DM_DELINQUENT_ACCOUNT_INFO_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    NULL X_COLL_STATUS
    ,NULL X_LAST_UPD_COLL_BALANCE
    ,NULL X_LAST_UPD_COLL_DATETIME
    ,NULL X_ORIG_COLL_BALANCE
    ,NULL X_ORIG_COLL_DATETIME
    ,NULL X_ETC_ACCNT_ID
    ,NULL X_REVOKE_BALANCE
    ,NULL X_RVKD_FINL_DT
    ,NULL X_RVK_TAGDP_FRFIT
    ,NULL X_RVK_TAGLST_FEE
    ,NULL X_REVOKE_FEE
    ,NULL X_RVKD_WRN_DT
    ,NULL X_COLL_EXCLUDE_FLG
    ,NULL X_COLL_SETTLE_BALANCE
    ,NULL X_COLL_SETTLE_DATETIME
    ,NULL X_COLL_SETTLE_DISMISSAL
    ,NULL X_COLL_SETTLE_PAYMENT
    ,NULL X_COLL_SETTLE_UNCOLLECTABLE
    ,NULL ISSUE_NUM
    ,NULL X_GD_STATUS_DT
    ,NULL X_NBL_LTTR_CNT
    ,NULL X_NBL_LTTR_DT
    ,NULL X_REVOKE_EXCLUDE
    ,NULL X_WRN_LTTR_CNT
    ,NULL X_ZRO_STATUS_DT
    ,NULL ACCNT_STATUS
    ,NULL UPDATE_TS
    ,NULL X_COLL_LAST_DISP_CODE
    ,NULL WRITE_OFF_FLAG
    ,NULL PROCESSING_DATE
    ,NULL WOFF_REJECT_REASON
    ,NULL ACTUAL_WRITEOFF_DATE
    ,NULL FINAL_WOFF_AMOUNT
    ,NULL SOURCE_SYSTEM
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_DELINQUENT_ACCOUNT_INFO_tab
    LIMIT P_ARRAY_SIZE;
    EXIT WHEN C1%NOTFOUND;

    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_DELINQUENT_ACCOUNT_INFO_tab.first .. DM_DELINQUENT_ACCOUNT_INFO_tab.last
           INSERT INTO DM_DELINQUENT_ACCOUNT_INFO VALUES DM_DELINQUENT_ACCOUNT_INFO_tab(i);
                       

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


