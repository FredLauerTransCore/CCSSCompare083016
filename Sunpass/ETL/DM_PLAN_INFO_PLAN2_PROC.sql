/********************************************************
*
* Name: DM_PLAN_INFO_PLAN2_PROC
* Created by: DT, 4/13/2016
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
    NULL ACCOUNT_NUMBER
    ,NULL PLAN_NAME
    ,NULL DEVICE_NUMBER
    ,NULL START_DATE
    ,NULL END_DATE
    ,NULL PLAN_STATUS
    ,NULL AUTO_RENEW
    ,NULL CREATED
    ,NULL CREATED_BY
    ,NULL LAST_UPD
    ,NULL LAST_UPD_BY
    ,NULL SOURCE_SYSTEM
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_PLAN_INFO_PLAN2_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

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


