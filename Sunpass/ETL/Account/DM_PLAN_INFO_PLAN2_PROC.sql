/********************************************************
*
* Name: DM_PLAN_INFO_PLAN2_PROC
* Created by: DT, 4/21/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_PLAN_INFO_PLAN2
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_PLAN_INFO_PLAN2_PROC IS

TYPE DM_PLAN_INFO_PLAN2_TYP IS TABLE OF DM_PLAN_INFO_PLAN2%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_PLAN_INFO_PLAN2_tab DM_PLAN_INFO_PLAN2_TYP;


P_ARRAY_SIZE NUMBER:=10000;


CURSOR C1 IS SELECT 
    ACCT_ACCT_NUM ACCOUNT_NUMBER
    ,'BAYWAY ISLES' PLAN_NAME
    ,INVTRANSP_TRANSP_TRANSP_ID DEVICE_NUMBER
    ,BAYWAY_PASS1_DATE START_DATE
    ,CASE END_DATE
    ,BAYWAY_PASS_PAID1 PLAN_STATUS
    ,'N' AUTO_RENEW
    ,BAYWAY_PASS_PAID1 CREATED
    , CREATED_BY
    ,BAYWAY_PASS_PAID1 LAST_UPD
    , LAST_UPD_BY
    ,'SUNPASS' SOURCE_SYSTEM
FROM PATRON.PA_ACCT_TRANSP 
WHERE BAYWAY_PASS1_PAID= 'Y';

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PLAN_INFO_PLAN2_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN */

    /*ETL SECTION END   */

    /*Bulk insert */ 
    FORALL i in DM_PLAN_INFO_PLAN2_tab.first .. DM_PLAN_INFO_PLAN2_tab.last
           INSERT INTO DM_PLAN_INFO_PLAN2 VALUES DM_PLAN_INFO_PLAN2_tab(i);
                       
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

