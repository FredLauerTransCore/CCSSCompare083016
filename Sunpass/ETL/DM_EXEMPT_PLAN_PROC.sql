/********************************************************
*
* Name: DM_EXEMPT_PLAN_PROC
* Created by: DT, 4/13/2016
* Revision: 1.0
* Description: This is the template for bulk read/write
*              DM_EXEMPT_PLAN
*
********************************************************/

set serveroutput on
set verify on
set echo on

CREATE OR REPLACE PROCEDURE DM_EXEMPT_PLAN_PROC IS

TYPE DM_EXEMPT_PLAN_TYP IS TABLE OF DM_EXEMPT_PLAN%ROWTYPE 
     INDEX BY BINARY_INTEGER;
DM_EXEMPT_PLAN_tab DM_EXEMPT_PLAN_TYP;


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
    ,NULL PLATE_STATE
    ,NULL PLATE_COUNTRY
    ,NULL PLATE_NUMBER
FROM FTE_TABLE; /*Change FTE_TABLE to the actual table name*/

BEGIN
 
  OPEN C1;  

  LOOP

    /*Bulk select */
    FETCH C1 BULK COLLECT INTO DM_EXEMPT_PLAN_tab
    LIMIT P_ARRAY_SIZE;


    /*ETL SECTION BEGIN

      ETL SECTION END*/

    /*Bulk insert */ 
    FORALL i in DM_EXEMPT_PLAN_tab.first .. DM_EXEMPT_PLAN_tab.last
           INSERT INTO DM_EXEMPT_PLAN VALUES DM_EXEMPT_PLAN_tab(i);
                       
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


