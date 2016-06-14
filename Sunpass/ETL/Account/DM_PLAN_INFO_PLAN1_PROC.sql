/********************************************************
*
* Name: DM_PLAN_INFO_PLAN1_PROC
* Created by: DT, 4/21/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PLAN_INFO_PLAN1
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PLAN_INFO_PLAN1_PROC IS

TYPE DM_PLAN_INFO_PLAN1_TYP IS TABLE OF DM_PLAN_INFO_PLAN1%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PLAN_INFO_PLAN1_tab DM_PLAN_INFO_PLAN1_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,'BAYWAY ISLES' PLAN_NAME
    ,INVTRANSP_TRANSP_TRANSP_ID DEVICE_NUMBER
    ,BAYWAY_PASS1_DATE START_DATE
    ,CASE
       WHEN TO_CHAR(BAYWAY_PASS2_DATE,'MMDD') >'0701' THEN
       add_months(to_date('06/30/'||TO_CHAR(BAYWAY_PASS2_DATE,'YYYY'),'MM/DD/YYYY'),12)
       WHEN TO_CHAR(BAYWAY_PASS2_DATE,'MMDD') <='0701' THEN
       to_date('06/30/'||TO_CHAR(BAYWAY_PASS2_DATE,'YYYY'),'MM/DD/YYYY')
     END
     END_DATE
    ,BAYWAY_PASS1_PAID PLAN_STATUS
    ,'N' AUTO_RENEW
    ,BAYWAY_PASS1_PAID CREATED
    ,'SUNPASS_CSC_ID' CREATED_BY
    ,BAYWAY_PASS1_PAID LAST_UPD
    ,'SUNPASS_CSC_ID' LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
FROM PA_ACCT_TRANSP
WHERE BAYWAY_PASS1_PAID= 'Y' or BAYWAY_PASS2_PAID = 'Y';

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PLAN_INFO_PLAN1_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_PLAN_INFO_PLAN1_tab.first .. DM_PLAN_INFO_PLAN1_tab.last
           INSERT INTO DM_PLAN_INFO_PLAN1 VALUES DM_PLAN_INFO_PLAN1_tab(i);
                       
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


